use crate::{
    error::ErrorReport,
    generics::{ColumnDefault, FieldMetadata, GenericSqlRow, GenericWrapper},
};
use deadpool::managed::Object;
use futures::{pin_mut, Future, Stream, StreamExt};
pub use rusqlite::ToSql;
use rusqlite::Transaction;
use std::{collections::HashMap, path::Path};
use xlsxreader::Reader;

pub type Connection = Object<ConnectionManager>;

pub struct ConnectionManager {
    file_path: String,
}

impl ConnectionManager {
    pub fn init(file_path: String) -> Self {
        Self { file_path }
    }

    pub fn get_file_path(&self) -> &String {
        &self.file_path
    }
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for ConnectionManager {
    type Type = rusqlite::Connection;
    type Error = ErrorReport;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let conn = rusqlite::Connection::open(&self.file_path)?;
        Ok(conn)
    }

    async fn recycle(
        &self,
        client: &mut Self::Type,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        if client.is_busy() {
            Err(deadpool::managed::RecycleError::Message(
                "connection is busy".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

pub struct Pool {
    inner: deadpool::managed::Pool<ConnectionManager>,
    batch_size: usize,
}

impl Pool {
    pub fn init(
        file_path: String,
        max_size: usize,
        batch_size: usize,
    ) -> Result<Self, ErrorReport> {
        let manager = ConnectionManager::init(file_path);
        Ok(Self {
            inner: deadpool::managed::Pool::builder(manager)
                .max_size(max_size)
                .build()
                .map_err(|error| match error {
                    deadpool::managed::BuildError::Backend(err) => err,
                    deadpool::managed::BuildError::NoRuntimeSpecified(v) => {
                        crate::error::error_deadpool_no_runtime(&v)
                    }
                })?,
            batch_size,
        })
    }

    pub fn get_batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn get_ref(&self) -> &deadpool::managed::Pool<ConnectionManager> {
        &self.inner
    }

    pub async fn conn_get<T, F, R>(&self, callable: F) -> Result<Vec<T>, ErrorReport>
    where
        for<'c> T: TryFrom<&'c rusqlite::Row<'c>, Error = ErrorReport>,
        F: FnOnce(Connection) -> R,
        R: Future<Output = Result<Vec<T>, ErrorReport>>,
    {
        let conn = self
            .inner
            .get()
            .await
            .map_err(|err| crate::error::error_database_connection_pool(err.into()))?;
        callable(conn).await
    }

    pub async fn conn_run<F, R>(&self, callable: F) -> Result<usize, ErrorReport>
    where
        F: FnOnce(Connection) -> R,
        R: Future<Output = Result<usize, ErrorReport>>,
    {
        let conn = self
            .inner
            .get()
            .await
            .map_err(|err| crate::error::error_database_connection_pool(err.into()))?;
        callable(conn).await
    }
}

pub fn connection_get<'a, 'b: 'a, R>(
    conn: &'a Object<ConnectionManager>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
) -> Result<Vec<R>, ErrorReport>
where
    for<'c> R: TryFrom<&'c rusqlite::Row<'c>, Error = ErrorReport>,
{
    let params = if let Some(p) = param_values { p } else { &[] };
    let mut stmt = conn.prepare(query_string)?;
    let mut rows = stmt.query(params)?;

    let mut res = Vec::new();
    while let Some(row) = rows.next()? {
        res.push(R::try_from(row)?);
    }
    Ok(res)
}

pub fn connection_run<'a, 'b: 'a>(
    conn: &'a Object<ConnectionManager>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
) -> Result<usize, ErrorReport> {
    let params = if let Some(p) = param_values { p } else { &[] };
    let mut stmt = conn.prepare(query_string)?;
    let res = stmt.execute(params)?;
    Ok(res)
}

pub fn transaction_get<'a, 'b: 'a, R>(
    transaction: &'a Transaction<'a>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
) -> Result<Vec<R>, ErrorReport>
where
    for<'c> R: TryFrom<&'c rusqlite::Row<'c>, Error = ErrorReport>,
{
    let params = if let Some(p) = param_values { p } else { &[] };
    let mut stmt = transaction.prepare(query_string)?;
    let mut rows = stmt.query(params)?;

    let mut res = Vec::new();
    while let Some(row) = rows.next()? {
        res.push(R::try_from(row)?);
    }
    Ok(res)
}

pub fn transaction_run<'a, 'b: 'a>(
    transaction: &'a Transaction<'a>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
) -> Result<usize, ErrorReport> {
    let params = if let Some(p) = param_values { p } else { &[] };
    let mut stmt = transaction.prepare(query_string)?;
    let res = stmt.execute(params)?;
    Ok(res)
}

pub fn batch_get<'a, 'b: 'a, R>(
    transaction: &'a Transaction<'a>,
    query_string: &'a str,
    param_values: impl Iterator<Item = Vec<Box<dyn ToSql>>>,
) -> Result<Vec<R>, ErrorReport>
where
    for<'c> R: TryFrom<&'c rusqlite::Row<'c>, Error = ErrorReport>,
{
    let mut stmt = transaction.prepare(query_string)?;
    let mut res: Vec<R> = Vec::new();
    for p in param_values {
        let mut rows = stmt.query(rusqlite::params_from_iter(p.iter()))?;
        while let Some(row) = rows.next()? {
            res.push(R::try_from(row)?);
        }
    }
    Ok(res)
}

pub fn batch_run<'a, 'b: 'a>(
    transaction: &'a Transaction<'a>,
    query_string: &'a str,
    param_values: impl Iterator<Item = Vec<Box<dyn ToSql>>>,
) -> Result<usize, ErrorReport> {
    let mut stmt = transaction.prepare(query_string)?;
    let mut res: usize = 0;
    for p in param_values {
        res += stmt.execute(rusqlite::params_from_iter(p.iter()))?;
    }
    Ok(res)
}

pub async fn batch_run_from_json<'a, 'b: 'a>(
    transaction: &'a Transaction<'a>,
    batch_size: usize,
    query_string: &'a str,
    param_values: impl Stream<Item = impl serde::Serialize + 'b>,
) -> Result<usize, ErrorReport> {
    let mut stmt = transaction.prepare(query_string)?;

    let mut res: usize = 0;
    let mut contor: usize = 0;
    let mut recs = Vec::new();
    pin_mut!(param_values);
    while let Some(p) = param_values.next().await {
        recs.push(p);
        contor += 1;

        if contor % batch_size != 0 {
            continue;
        }

        res += stmt
            .execute(rusqlite::params![serde_json::json!(&recs)])
            .map_err(|err| {
                crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
            })?;
        recs.clear();
    }
    if !recs.is_empty() {
        res += stmt
            .execute(rusqlite::params![serde_json::json!(&recs)])
            .map_err(|err| {
                crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
            })?;
        recs.clear();
    }

    Ok(res)
}

fn table_meta(
    conn: &Object<ConnectionManager>,
    table_name: &str,
) -> Result<Vec<FieldMetadata>, ErrorReport> {
    let sql = r"select * from pragma_table_info(?);";
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(rusqlite::params![table_name])?;

    let mut res = Vec::new();
    while let Some(row) = rows.next()? {
        res.push(row.try_into()?);
    }
    if res.is_empty() {
        return Err(crate::error::error_no_table_meta(&"", &table_name));
    }
    Ok(res)
}

fn _upload_sql_string<'a, 'b: 'a>(
    table_name: &'a str,
    table_meta: &'b HashMap<String, FieldMetadata>,
    restricted_columns: Option<&'b HashMap<String, ColumnDefault<'b, GenericWrapper>>>,
    upsert_constraint_columns: Option<&'b [&'b str]>,
) -> Result<String, ErrorReport> {
    let cols_rstr = match restricted_columns {
        Some(cols) => cols.to_owned(),
        None => HashMap::new(),
    };

    let cols_alwd = table_meta
        .iter()
        .filter(|(_, v)| !cols_rstr.contains_key(&v.name()))
        .map(|(_, v)| v)
        .collect::<Vec<_>>();

    // insert sql statement components
    //------------------------------
    let ins_col_list_alwd = cols_alwd
        .iter()
        .map(|val| format!(r#""{}""#, val.name()))
        .collect::<Vec<_>>()
        .join(",");

    let ins_col_list_rstr = {
        let res = cols_rstr
            .iter()
            .filter(|(_, v)| !matches!(v, ColumnDefault::ByDatabase))
            .map(|(k, _)| format!(r#""{}""#, k))
            .collect::<Vec<_>>()
            .join(",");

        if res.is_empty() {
            res
        } else {
            format!(",{}", res)
        }
    };

    let sel_col_list_alwd = cols_alwd
        .iter()
        .map(|val| format!(r#"a.value ->> '$."{}"'"#, val.name()))
        .collect::<Vec<_>>()
        .join(",");

    let sel_col_list_rstr = {
        let res = cols_rstr
            .iter()
            .filter(|(_, v)| !matches!(v, ColumnDefault::ByDatabase))
            .map(|(k, v)| {
                if let ColumnDefault::Formula(formula) = v {
                    formula.to_string()
                } else {
                    format!(r#"a.value ->> '$."{}"'"#, k)
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        if res.is_empty() {
            res
        } else {
            format!(",{}", res)
        }
    };

    // update sql statement components
    //------------------------------
    let constr_cols = if let Some(val) = upsert_constraint_columns {
        val
    } else {
        &[]
    };

    let upsert_stmt = if constr_cols.is_empty() {
        "".to_string()
    } else {
        let constr_text = constr_cols
            .iter()
            .map(|val| val.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let excl_cols = cols_rstr
            .iter()
            .filter(|(_, v)| matches!(v, ColumnDefault::ByDatabase))
            .map(|(k, _)| k.to_owned())
            .collect::<Vec<_>>();

        let upd_col_list_alwd = cols_alwd
            .iter()
            .filter(|v| !constr_cols.contains(&v.name().as_str()) && !excl_cols.contains(&v.name()))
            .map(|v| format!(r#""{}" = excluded."{}""#, v.name(), v.name()))
            .collect::<Vec<_>>()
            .join(",");

        let upd_col_list_rstr = {
            let res = cols_rstr
                .iter()
                .filter(|(k, _)| !constr_cols.contains(&k.as_str()) && !excl_cols.contains(k))
                .map(|(k, v)| match v {
                    ColumnDefault::Value(_) => format!(r#""{}" = excluded."{}""#, k, k),
                    ColumnDefault::Formula(f) => format!(r#""{}" = {}"#, k, f),
                    _ => "".to_string(),
                })
                .collect::<Vec<_>>()
                .join(",");

            if res.is_empty() {
                res
            } else {
                format!(",{}", res)
            }
        };

        format!(
            "on conflict ({}) do update set {}{}",
            constr_text, upd_col_list_alwd, upd_col_list_rstr
        )
    };

    // full upsert statement
    //------------------------------
    Ok(format!(
        r#"insert into "{}" ({}{}) select {}{} from json_each(?) as a {};"#,
        table_name,
        ins_col_list_alwd,
        ins_col_list_rstr,
        sel_col_list_alwd,
        sel_col_list_rstr,
        upsert_stmt
    ))
}

pub fn upload_from_text_file<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    batch_size: usize,
    table_name: &'a str,
    upsert_constraint_columns: Option<&'b [&'b str]>,
    restricted_columns: Option<&'b HashMap<String, ColumnDefault<'b, GenericWrapper>>>,
    file_path: &'b Path,
    file_column_delimiter: u8,
    file_column_quote_char: Option<u8>,
    file_quote_char_escape: Option<u8>,
) -> Result<usize, ErrorReport> {
    let metadata = table_meta(conn, table_name)?
        .into_iter()
        .map(|v| (v.name(), v))
        .collect();

    let sql = _upload_sql_string(
        table_name,
        &metadata,
        restricted_columns,
        upsert_constraint_columns,
    )?;

    let restricted_columns = if let Some(map) = restricted_columns {
        let map = map
            .iter()
            .filter(|(_, v)| matches!(v, ColumnDefault::Value(_)))
            .map(|x| x.to_owned())
            .collect::<HashMap<_, _>>();
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
    } else {
        None
    };

    let mut csv_read_builder = csv::ReaderBuilder::new();
    csv_read_builder.has_headers(true);
    csv_read_builder.delimiter(file_column_delimiter);
    csv_read_builder.trim(csv::Trim::All);
    csv_read_builder.flexible(false);

    if let Some(val) = file_column_quote_char {
        csv_read_builder.quote(val);
        csv_read_builder.quoting(true);
    } else {
        csv_read_builder.quoting(false);
    }
    csv_read_builder.escape(file_quote_char_escape);
    let mut csv_reader = csv_read_builder.from_path(file_path)?;

    let tx = conn.transaction()?;
    let mut stmt = tx.prepare(sql.as_str())?;
    let mut res: usize = 0;
    let mut contor: usize = 0;
    let mut recs: Vec<HashMap<String, GenericWrapper>> = Vec::new();
    for row in csv_reader.deserialize() {
        let mut record: HashMap<String, GenericWrapper> = match row {
            Ok(val) => val,
            Err(err) => return Err(crate::error::error_csv_read_row(&res, Box::new(err))),
        };

        //add fields with values from restricted fields
        if let Some(rstr) = &restricted_columns {
            for (k, v) in rstr.iter() {
                if let ColumnDefault::Value(v) = v {
                    record.insert(k.to_string(), v.to_owned());
                }
            }
        }

        //clean None values from the record
        record = record
            .drain()
            .filter(|(_, v)| !matches!(v, GenericWrapper::None))
            .collect();

        recs.push(record);
        contor += 1;

        if contor % batch_size != 0 {
            continue;
        }

        res += stmt
            .execute(rusqlite::params![serde_json::json!(&recs)])
            .map_err(|err| {
                crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
            })?;
        recs.clear();
    }
    if !recs.is_empty() {
        res += stmt
            .execute(rusqlite::params![serde_json::json!(&recs)])
            .map_err(|err| {
                crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
            })?;
        recs.clear();
    }
    drop(stmt);
    tx.commit()?;
    Ok(res)
}

pub fn download_to_csv<'a, 'b: 'a>(
    conn: &'a Object<ConnectionManager>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
    file_path: &'b Path,
) -> Result<usize, ErrorReport> {
    let params = if let Some(p) = param_values { p } else { &[] };
    let mut stmt = conn.prepare(query_string)?;
    let mut rows = stmt.query(params)?;

    let mut writer = csv::Writer::from_path(file_path)?;

    let mut res: usize = 0;
    while let Some(row) = rows.next()? {
        let rec: GenericSqlRow<String, GenericWrapper> = row.try_into()?;
        if res == 0 {
            let header: Vec<_> = rec.as_ref().iter().map(|(k, _)| k).collect();
            writer.write_record(header)?;
        }
        let vals: Vec<_> = rec.as_ref().iter().map(|(_, v)| v).collect();
        writer.serialize(vals)?;
        res += 1;
    }
    writer.flush()?;
    Ok(res)
}

pub fn upload_from_xlsx_file<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    batch_size: usize,
    table_name: &'a str,
    upsert_constraint_columns: Option<&'b [&'b str]>,
    restricted_columns: Option<&'b HashMap<String, ColumnDefault<'b, GenericWrapper>>>,
    file_path: &'b Path,
    sheet_name: Option<&'a str>,
) -> Result<usize, ErrorReport> {
    let metadata = table_meta(conn, table_name)?
        .into_iter()
        .map(|v| (v.name(), v))
        .collect();

    let sql = _upload_sql_string(
        table_name,
        &metadata,
        restricted_columns,
        upsert_constraint_columns,
    )?;

    let restricted_columns = if let Some(map) = restricted_columns {
        let map = map
            .iter()
            .filter(|(_, v)| matches!(v, ColumnDefault::Value(_)))
            .map(|x| x.to_owned())
            .collect::<HashMap<_, _>>();
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
    } else {
        None
    };

    let mut workbook: xlsxreader::Xlsx<_> = xlsxreader::open_workbook(file_path)?;
    let worksheet = match if let Some(v) = sheet_name {
        workbook.worksheet_range(v)
    } else {
        workbook.worksheet_range_at(0)
    } {
        Some(Ok(w)) => w,
        Some(Err(err)) => return Err(err.into()),
        None => return Err(crate::error::error_xlsx_no_sheet()),
    };

    let Some((start_row, start_col)) = worksheet.start() else {
        return Err(crate::error::error_xlsx_empty_sheet());
    };
    let Some((end_row, end_col)) = worksheet.end() else {
        return Err(crate::error::error_xlsx_empty_sheet());
    };
    if end_row <= start_row || end_col < start_col {
        return Err(crate::error::error_xlsx_empty_sheet());
    };

    let mut header: Vec<String> = Vec::new();
    for cx in start_col..=end_col {
        let Some(v) = worksheet.get_value((start_row, cx)) else {
            return Err(crate::error::error_xlsx_header(&start_row, &cx));
        };
        let s = match v {
            xlsxreader::DataType::DateTime(_) => {
                if let Some(p) = v.as_datetime() {
                    p.to_string()
                } else {
                    v.to_string()
                }
            }
            xlsxreader::DataType::Error(_) => {
                return Err(crate::error::error_xlsx_cell_value(&start_row, &cx))
            }
            xlsxreader::DataType::Empty => {
                return Err(crate::error::error_xlsx_header(&start_row, &cx))
            }
            _ => v.to_string(),
        };
        header.push(s);
    }

    let tx = conn.transaction()?;
    let mut stmt = tx.prepare(sql.as_str())?;
    let mut res: usize = 0;
    let mut contor: usize = 0;
    let mut recs: Vec<HashMap<String, GenericWrapper>> = Vec::new();
    for rx in start_row + 1..=end_row {
        let mut record: HashMap<String, GenericWrapper> = HashMap::new();
        for cx in start_col..=end_col {
            let Some(col_name) = header.get(usize::try_from(cx)?) else {
                return Err(crate::error::error_xlsx_column_name(&cx));
            };
            let col_type = if let Some(f) = metadata.get(col_name) {
                f.sqlite_type_name().to_lowercase().trim().replace(' ', "")
            } else {
                return Err(crate::error::error_xlsx_column_sql_type(&cx));
            };
            let v = if let Some(p) = worksheet.get_value((rx, cx)) {
                p
            } else {
                &xlsxreader::DataType::Empty
            };

            let gv = if col_type.eq("time") {
                if let Some(p) = v.as_time() {
                    GenericWrapper::NaiveTime(p)
                } else {
                    crate::helper::naive_xlsx_cast(v, rx, cx)?
                }
            } else if col_type.eq("date") {
                if let Some(p) = v.as_date() {
                    GenericWrapper::NaiveDate(p)
                } else {
                    crate::helper::naive_xlsx_cast(v, rx, cx)?
                }
            } else if col_type.starts_with("datetime") || col_type.starts_with("timestamp") {
                if let Some(p) = v.as_datetime() {
                    GenericWrapper::NaiveTimestamp(p)
                } else {
                    crate::helper::naive_xlsx_cast(v, rx, cx)?
                }
            } else if col_type.starts_with("char")
                || col_type.starts_with("nchar")
                || col_type.starts_with("text")
                || col_type.starts_with("varchar")
                || col_type.starts_with("nvarchar")
                || col_type.starts_with("name")
                || col_type.starts_with("xml")
                || col_type.starts_with("clob")
            {
                crate::helper::naive_xlsx_cast(v, rx, cx)?
            } else if col_type.starts_with("float")
                || col_type.starts_with("real")
                || col_type.starts_with("double")
                || col_type.starts_with("decimal")
                || col_type.starts_with("numeric")
            {
                match v {
                    xlsxreader::DataType::String(p) => {
                        if let Ok(d) = rust_decimal::Decimal::from_str_exact(p) {
                            GenericWrapper::Decimal(d)
                        } else if let Ok(d) = p.parse() {
                            GenericWrapper::F64(d)
                        } else if let Ok(d) = p.parse() {
                            GenericWrapper::I64(d)
                        } else {
                            GenericWrapper::from(p)
                        }
                    }
                    _ => crate::helper::naive_xlsx_cast(v, rx, cx)?,
                }
            } else if col_type.eq("json") {
                match v {
                    xlsxreader::DataType::Int(p) => {
                        GenericWrapper::Json(serde_json::Value::from(*p))
                    }
                    xlsxreader::DataType::Float(p) => {
                        GenericWrapper::Json(serde_json::Value::from(*p))
                    }
                    xlsxreader::DataType::String(p) => {
                        if let Ok(d) = serde_json::from_str(p) {
                            GenericWrapper::Json(d)
                        } else {
                            GenericWrapper::from(p)
                        }
                    }
                    xlsxreader::DataType::Bool(p) => {
                        GenericWrapper::Json(serde_json::Value::from(*p))
                    }
                    xlsxreader::DataType::DateTime(p) => {
                        if let Some(d) = v.as_datetime() {
                            GenericWrapper::NaiveTimestamp(d)
                        } else {
                            GenericWrapper::Json(serde_json::Value::from(*p))
                        }
                    }
                    _ => crate::helper::naive_xlsx_cast(v, rx, cx)?,
                }
            } else if col_type.starts_with("uuid") {
                match v {
                    xlsxreader::DataType::String(p) => {
                        if let Ok(d) = uuid::Uuid::parse_str(p) {
                            GenericWrapper::Uuid(d)
                        } else {
                            GenericWrapper::from(p)
                        }
                    }
                    _ => crate::helper::naive_xlsx_cast(v, rx, cx)?,
                }
            } else if col_type.eq("int")
                || col_type.eq("int2")
                || col_type.eq("int4")
                || col_type.eq("int8")
                || col_type.eq("integer")
                || col_type.eq("tinyint")
                || col_type.eq("smallint")
                || col_type.eq("bigint")
            {
                match v {
                    xlsxreader::DataType::Float(p) => {
                        if p.fract() == 0f64 {
                            GenericWrapper::I64(*p as i64)
                        } else {
                            GenericWrapper::F64(*p)
                        }
                    }
                    xlsxreader::DataType::String(p) => GenericWrapper::I64(p.parse()?),
                    _ => crate::helper::naive_xlsx_cast(v, rx, cx)?,
                }
            } else {
                crate::helper::naive_xlsx_cast(v, rx, cx)?
            };
            record.insert(col_name.to_owned(), gv);
        }
        //add fields with values from restricted fields
        if let Some(rstr) = &restricted_columns {
            for (k, v) in rstr.iter() {
                if let ColumnDefault::Value(v) = v {
                    record.insert(k.to_string(), v.to_owned());
                }
            }
        }

        //clean None values from the record
        record = record
            .drain()
            .filter(|(_, v)| !matches!(v, GenericWrapper::None))
            .collect();

        recs.push(record);
        contor += 1;

        if contor % batch_size != 0 {
            continue;
        }

        res += stmt
            .execute(rusqlite::params![serde_json::json!(&recs)])
            .map_err(|err| {
                crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
            })?;
        recs.clear();
    }
    if !recs.is_empty() {
        res += stmt
            .execute(rusqlite::params![serde_json::json!(&recs)])
            .map_err(|err| {
                crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
            })?;
        recs.clear();
    }
    drop(stmt);
    tx.commit()?;
    Ok(res)
}

pub fn download_to_xlsx<'a, 'b: 'a>(
    conn: &'a Object<ConnectionManager>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
    file_path: &'b Path,
    sheet_name: Option<&'a str>,
) -> Result<usize, ErrorReport> {
    let params = if let Some(p) = param_values { p } else { &[] };
    let mut stmt = conn.prepare(query_string)?;
    let mut rows = stmt.query(params)?;

    let mut workbook = xlsxwriter::Workbook::new();
    let worksheet = workbook.add_worksheet();
    let sh_name = if let Some(v) = sheet_name {
        v
    } else {
        "Sheet1"
    };
    worksheet.set_name(sh_name)?;
    let formats = crate::helper::XlsxFormats::new();

    let mut res: usize = 0;
    let mut col_types: Vec<Option<String>> = Vec::new();
    while let Some(row) = rows.next()? {
        if res == 0 {
            col_types = row
                .as_ref()
                .columns()
                .iter()
                .map(|c| {
                    c.decl_type()
                        .map(|v| v.to_lowercase().trim().replace(' ', ""))
                })
                .collect();
            for i in 0..col_types.len() {
                worksheet.write_string(
                    0,
                    u16::try_from(i)?,
                    row.as_ref().column_name(i)?,
                    &formats.bold,
                )?;
            }
        }
        for i in 0..col_types.len() {
            let Some(ty) = col_types.get(i) else {
                return Err(crate::error::error_xlsx_column_sql_type(&i));
            };

            let Some(t) = ty else {
                match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => {}
                    rusqlite::types::ValueRef::Integer(v) => {
                        if let Ok(p) = i32::try_from(v) {
                            worksheet.write_number(
                                u32::try_from(res + 1)?,
                                u16::try_from(i)?,
                                p,
                                &formats.integer,
                            )?;
                        } else if let Ok(p) = u32::try_from(v) {
                            worksheet.write_number(
                                u32::try_from(res + 1)?,
                                u16::try_from(i)?,
                                p,
                                &formats.integer,
                            )?;
                        } else {
                            worksheet.write_string(
                                u32::try_from(res + 1)?,
                                u16::try_from(i)?,
                                v.to_string().as_str(),
                                &formats.integer,
                            )?;
                        }
                    }
                    rusqlite::types::ValueRef::Real(v) => {
                        worksheet.write_number(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            v,
                            &formats.decimal,
                        )?;
                    }
                    rusqlite::types::ValueRef::Text(v) => {
                        worksheet.write_string_only(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            &String::from_utf8_lossy(v),
                        )?;
                    }
                    rusqlite::types::ValueRef::Blob(_) => {
                        return Err(crate::error::error_sql_to_xlsx_value(
                            &"BLOB",
                            &row.as_ref().column_name(i)?,
                        ))
                    }
                }
                continue;
            };

            if t.starts_with("bool") {
                if let Some(v) = row.get::<_, Option<bool>>(i)? {
                    worksheet.write_boolean_only(u32::try_from(res + 1)?, u16::try_from(i)?, v)?;
                }
            } else if t.eq("time") {
                if let Some(v) = row.get::<_, Option<chrono::NaiveTime>>(i)? {
                    worksheet.write_time(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        v,
                        &formats.time,
                    )?;
                } else {
                    worksheet.write_blank(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        &formats.time,
                    )?;
                }
            } else if t.eq("date") {
                if let Some(v) = row.get::<_, Option<chrono::NaiveDate>>(i)? {
                    worksheet.write_date(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        v,
                        &formats.date,
                    )?;
                } else {
                    worksheet.write_blank(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        &formats.date,
                    )?;
                }
            } else if t.starts_with("datetime") || t.starts_with("timestamp") {
                if let Some(v) = row.get::<_, Option<chrono::NaiveDateTime>>(i)? {
                    worksheet.write_datetime(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        v,
                        &formats.timestamp,
                    )?;
                } else {
                    worksheet.write_blank(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        &formats.timestamp,
                    )?;
                }
            } else if t.starts_with("char")
                || t.starts_with("nchar")
                || t.starts_with("text")
                || t.starts_with("varchar")
                || t.starts_with("nvarchar")
                || t.starts_with("name")
                || t.starts_with("xml")
                || t.starts_with("clob")
            {
                if let Some(v) = row.get::<_, Option<String>>(i)? {
                    worksheet.write_string_only(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        v.as_str(),
                    )?;
                }
            } else if t.eq("uuid") {
                if let Some(v) = row.get::<_, Option<uuid::Uuid>>(i)? {
                    worksheet.write_string_only(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        v.to_string().as_str(),
                    )?;
                }
            } else if t.eq("json") {
                if let Some(v) = row.get::<_, Option<serde_json::Value>>(i)? {
                    worksheet.write_string_only(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        v.to_string().as_str(),
                    )?;
                }
            } else if t.starts_with("float")
                || t.starts_with("real")
                || t.starts_with("double")
                || t.starts_with("decimal")
                || t.starts_with("numeric")
            {
                if let Some(v) = row.get::<_, Option<f64>>(i)? {
                    worksheet.write_number(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        v,
                        &formats.decimal,
                    )?;
                } else {
                    worksheet.write_blank(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        &formats.decimal,
                    )?;
                }
            } else if t.eq("int")
                || t.eq("int2")
                || t.eq("int4")
                || t.eq("int8")
                || t.eq("integer")
                || t.eq("tinyint")
                || t.eq("smallint")
                || t.eq("bigint")
            {
                if let Some(v) = row.get::<_, Option<i64>>(i)? {
                    if let Ok(p) = i32::try_from(v) {
                        worksheet.write_number(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            p,
                            &formats.integer,
                        )?;
                    } else if let Ok(p) = u32::try_from(v) {
                        worksheet.write_number(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            p,
                            &formats.integer,
                        )?;
                    } else {
                        worksheet.write_string(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            v.to_string().as_str(),
                            &formats.integer,
                        )?;
                    }
                } else {
                    worksheet.write_blank(
                        u32::try_from(res + 1)?,
                        u16::try_from(i)?,
                        &formats.integer,
                    )?;
                }
            } else {
                return Err(crate::error::error_sql_to_xlsx_value(
                    t,
                    &row.as_ref().column_name(i)?,
                ));
            }
        }
        res += 1;
    }
    worksheet.set_freeze_panes(1, 0)?;
    workbook.save(file_path)?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use crate::generics::{ColumnDefault, GenericSqlRow, GenericWrapper};
    use futures::StreamExt;
    use std::{collections::HashMap, time::Duration};

    type R = Vec<GenericSqlRow<String, GenericWrapper>>;

    #[tokio::test]
    async fn connection() {
        let db_path = env!("SQLITE_DB_PATH");
        let conn = rusqlite::Connection::open(db_path).unwrap();
        let mut stmt = conn.prepare("select 1 as result;").unwrap();
        let rows = stmt.query_and_then([], |row| row.get::<_, i64>(0)).unwrap();
        let mut res: i64 = 0;
        for row in rows {
            res = row.unwrap();
        }
        assert_eq!(1, res);
    }

    #[tokio::test]
    async fn pool() {
        let db_path = env!("SQLITE_DB_PATH");
        let pool = super::Pool::init(db_path.to_string(), 5, 3000).unwrap();

        for _ in 0..5 {
            let conn = pool.get_ref().get().await.unwrap();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let mut stmt = conn.prepare("select 1 as result;").unwrap();
                let _ = stmt.query([]).unwrap();
            });
        }

        let pool_size = pool.get_ref().status().size;
        assert_eq!(5, pool_size, "Pool not of configured size");
    }

    fn get_pool() -> super::Pool {
        let db_path = env!("SQLITE_DB_PATH");
        super::Pool::init(db_path.to_string(), 5, 3000).unwrap()
    }

    #[tokio::test]
    async fn query_result() {
        let pool = get_pool();

        let param_values: &[&dyn super::ToSql] = &[
            &1_i32,
            &"primul",
            &chrono::NaiveDate::from_ymd_opt(2022, 1, 1),
        ];

        let future = |conn: super::Connection| async move {
            super::connection_get(
                &conn,
                r#"select ? as id, ? as "text", ? as "date";"#,
                Some(param_values),
            )
        };

        let recs: R = pool.conn_get(future).await.unwrap();
        let rec = recs.get(0).unwrap();
        assert_eq!(
            (
                &GenericWrapper::I64(1),
                &GenericWrapper::String("primul".to_owned()),
                &GenericWrapper::NaiveDate(chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap())
            ),
            (
                rec.as_ref().get(&"id".to_string()).unwrap(),
                rec.as_ref().get(&"text".to_string()).unwrap(),
                rec.as_ref().get(&"date".to_string()).unwrap(),
            ),
            "Record 1 did not match the query result"
        );

        let param_values: &[&dyn super::ToSql] =
            &[&2_i32, &None::<String>, &None::<chrono::NaiveDate>];

        let future = |conn| async move {
            super::connection_get(
                &conn,
                r#"select ? as id, ? as "text", ? as "date";"#,
                Some(param_values),
            )
        };

        let recs: R = pool.conn_get(future).await.unwrap();
        let rec = recs.get(0).unwrap();
        assert_eq!(
            (
                &GenericWrapper::I64(2),
                &GenericWrapper::None,
                &GenericWrapper::None
            ),
            (
                rec.as_ref().get(&"id".to_string()).unwrap(),
                rec.as_ref().get(&"text".to_string()).unwrap(),
                rec.as_ref().get(&"date".to_string()).unwrap(),
            ),
            "Record 2 did not match the query result"
        );

        let future = |conn| async move {
            super::connection_get(
                &conn,
                r#"select 3 as id, 'al treilea' as "text", date('2022-01-03') as "date" where 1 = 2;"#,
                None,
            )
        };

        let recs: R = pool.conn_get(future).await.unwrap();
        assert_eq!(
            0,
            recs.len(),
            "Query result should have contained no record"
        );
    }

    #[tokio::test]
    async fn query_execute() {
        let pool = get_pool();

        let param_values: &[&dyn super::ToSql] = &[
            &1_i32,
            &"primul",
            &chrono::NaiveDate::from_ymd_opt(2022, 1, 1),
        ];

        let future = |conn| async move {
            super::connection_run(
                &conn,
                r#"select ? as id, ? as "text", ? as "date";"#,
                Some(param_values),
            )
        };

        let res = pool.conn_run(future).await;
        assert!(res.is_err(), "It should return error");

        let future = |conn| async move {
            super::connection_run(&conn, r"drop table if exists temp.tbl_tmp_tabela;", None)?;
            super::connection_run(
                &conn,
                r"create temporary table tbl_tmp_tabela (
                    id integer,
                    label text
                );",
                None,
            )?;

            let res = super::connection_run(
                &conn,
                r#"
                    insert into tbl_tmp_tabela (id, label)
                    select
                        id, label
                    from (
                        select 1 as id, 'string1' as label union
                        select 2 as id, 'string2' as label union
                        select 3 as id, 'string3' as label union
                        select 4 as id, 'string4' as label
                    ) as a;
                "#,
                None,
            )?;

            super::connection_run(&conn, r"drop table tbl_tmp_tabela;", None)?;
            Ok(res)
        };

        let res = pool.conn_run(future).await.unwrap();
        assert_eq!(4, res, "Execute count is not as expected");
    }

    #[tokio::test]
    async fn transaction_execute() {
        let pool = get_pool();

        let param_vals_1: &[&dyn super::ToSql] = &[
            &1_i32,
            &"primul",
            &chrono::NaiveDate::from_ymd_opt(2022, 1, 1),
        ];
        let param_vals_2: &[&dyn super::ToSql] =
            &[&2_i32, &"al doilea", &None::<chrono::NaiveDate>];

        let future = |mut conn: super::Connection| async move {
            let tx = conn.transaction()?;

            super::transaction_run(&tx, r#"drop table if exists tbl_tmp_tabela;"#, None)?;
            super::transaction_run(
                &tx,
                r#"create temp table tbl_tmp_tabela (id int, "text" text, "date" date);"#,
                None,
            )?;

            let mut res = super::transaction_run(
                &tx,
                r#"insert into tbl_tmp_tabela (id, "text", "date") values (?, ?, ?);"#,
                Some(param_vals_1),
            )?;
            res += super::transaction_run(
                &tx,
                r#"insert into tbl_tmp_tabela (id, "text", "date") values (?, ?, ?);"#,
                Some(param_vals_2),
            )?;
            tx.commit()?;

            Ok(res)
        };
        let res = pool.conn_run(future).await.unwrap();
        assert_eq!(
            2, res,
            "Transaction did not return the correct affected records number"
        );

        let future = |mut conn: super::Connection| async move {
            let tx = conn.transaction()?;

            super::transaction_run(
                &tx,
                r#"create temp table tbl_tmp_tabela (id int, "text" text, "date" date, constraint tbl_tmp_tabela_ck1 check (id <= 1));"#,
                None,
            )?;

            let res = super::transaction_run(
                &tx,
                r#"insert into tbl_tmp_tabela (id, "text", "date") values (?, ?, ?);"#,
                Some(param_vals_2),
            )?;
            tx.commit()?;
            Ok(res)
        };
        let res = pool.conn_run(future).await;
        assert!(
            res.err()
                .unwrap()
                .to_string()
                .contains("tbl_tmp_tabela_ck1"),
            "Not the correct error"
        );
    }

    #[tokio::test]
    async fn iterator_processing() {
        let pool = get_pool();
        let sql = r"
            insert into tbl_tst_values (field1, field2, field3, field4, field5, field6)
            values (?, ?, ?, ?, ?, ?)
            returning id;
        ";
        let mut contor = 0;
        let param_values = std::iter::from_fn(|| {
            contor += 1;
            if contor > 4700 {
                return None;
            }

            let res: Vec<Box<dyn super::ToSql>> = vec![
                Box::new(format!("text {}", contor)),
                Box::new(contor),
                Box::new(f64::from(contor)),
                Box::new(format!("string {}", contor)),
                Box::new(chrono::NaiveDate::from_ymd_opt(2022, 1, 1)),
                if contor % 2 == 0 {
                    Box::new(
                        chrono::NaiveDateTime::parse_from_str(
                            "2022-01-01 00:01:45",
                            "%Y-%m-%d %H:%M:%S",
                        )
                        .unwrap(),
                    )
                } else {
                    Box::new(None::<chrono::NaiveDateTime>)
                },
            ];
            Some(res)
        });

        let future = |mut conn: super::Connection| async move {
            let tx = conn.transaction()?;

            let res: R = super::batch_get(&tx, sql, param_values)?;
            tx.commit()?;
            Ok(res)
        };
        let recs = pool.conn_get(future).await.unwrap();
        assert_eq!(4700, recs.len(), "Result count is not as expected");
        if let GenericWrapper::I64(_) = recs
            .get(0)
            .unwrap()
            .as_ref()
            .get(&"id".to_string())
            .unwrap()
        {
            assert!(true)
        } else {
            assert!(false, "Result field is not of expected type")
        }

        let sql = r"delete from tbl_tst_values where id = ?;";
        let param_values = recs.iter().map(|x| {
            let res: Box<dyn super::ToSql> =
                Box::new(x.as_ref().get(&"id".to_string()).unwrap().clone());
            vec![res]
        });
        let future = |mut conn: super::Connection| async move {
            let tx = conn.transaction()?;
            let res = super::batch_run(&tx, sql, param_values)?;
            tx.commit()?;
            Ok(res)
        };
        let res = pool.conn_run(future).await.unwrap();
        assert_eq!(4700, res, "Deleted position count is not as expected");
    }

    #[tokio::test]
    async fn stream_processing() {
        let pool = get_pool();

        let sql = r"insert into tbl_tst_values (field1, field2, field3, field4, field5, field6)
            select a.value ->> '$.field1', a.value ->> '$.field2', a.value ->> '$.field3', a.value ->> '$.field4', a.value ->> '$.field5', a.value ->> '$.field6'
            from json_each(?) as a;
        ";

        let param_values = futures::stream::iter(0..100_000).map(move |i| {
            let mut map: HashMap<&str, GenericWrapper> = HashMap::new();
            map.insert("field1", "stream testing".into());
            map.insert("field2", i.into());
            map.insert("field3", rust_decimal::Decimal::from(i).into());
            if i % 2 == 0 {
                map.insert("field4", format!("text {}", i).into());
            }
            map.insert("field5", chrono::NaiveDate::from_ymd_opt(2022, 1, 1).into());
            if i % 2 == 0 {
                map.insert(
                    "field6",
                    chrono::NaiveDateTime::parse_from_str(
                        "2022-01-01 00:01:45",
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap()
                    .into(),
                );
            }
            map
        });
        let batch_size = pool.get_batch_size();

        let future = |mut conn: super::Connection| async move {
            let tx = conn.transaction()?;
            let res = super::batch_run_from_json(&tx, batch_size, sql, param_values).await?;
            tx.commit()?;
            Ok(res)
        };
        let res = pool.conn_run(future).await.unwrap();
        assert_eq!(100_000, res, "Insert count not as expected");

        let sql = r"delete from tbl_tst_values where field1 = 'stream testing';";
        let future = |conn| async move { super::connection_run(&conn, sql, None) };
        let res = pool.conn_run(future).await.unwrap();
        assert_eq!(100_000, res, "Delete count not as expected");
    }

    #[tokio::test]
    async fn table_meta() {
        let pool = get_pool();
        let future = |conn| async move { super::table_meta(&conn, "tbl_tst_users") };
        let res = pool.conn_get(future).await.unwrap();
        assert!(
            !res.is_empty(),
            "Did not get any column metadata for table 'tbl_tst_users'"
        );

        let table_meta: HashMap<_, _> = res.into_iter().map(|v| (v.name(), v)).collect();

        let mut restricted_columns: HashMap<String, ColumnDefault<GenericWrapper>> = HashMap::new();
        restricted_columns.insert("id".to_string(), ColumnDefault::ByDatabase);
        restricted_columns.insert("mod_de".to_string(), ColumnDefault::Value("C12153".into()));
        restricted_columns.insert(
            "mod_timp".to_string(),
            ColumnDefault::Formula("current_timestamp"),
        );

        let sql = super::_upload_sql_string(
            "tbl_tst_users",
            &table_meta,
            Some(&restricted_columns),
            Some(&["username"]),
        )
        .unwrap();
        assert!(
            sql.contains(r#"excluded."mod_de""#),
            "The resulted sql statement is illformed"
        );
    }

    #[tokio::test]
    async fn csv() {
        let pool = get_pool();

        let sql = r#"insert into tbl_tst_column_names ("Field 1", "Field.2", "fiEld 3", "Field_4", "FIELD 5", "Field 6")
            select a.value ->> '$."Field 1"', a.value ->> '$."Field.2"', a.value ->> '$."fiEld 3"', a.value ->> '$."Field_4"', a.value ->> '$."FIELD 5"', a.value ->> '$."Field 6"'
            from json_each(?) as a;
        "#;
        let param_values = futures::stream::iter(0..100_000).map(move |i| {
            let mut map: HashMap<&str, GenericWrapper> = HashMap::new();
            map.insert("Field 1", format!("string {}", i).into());
            map.insert("Field.2", i.into());
            map.insert("fiEld 3", rust_decimal::Decimal::from(i).into());
            if i % 2 == 0 {
                map.insert("Field_4", format!("text {}", i).into());
            }
            map.insert(
                "FIELD 5",
                chrono::NaiveDate::from_ymd_opt(2022, 1, 1).into(),
            );
            if i % 2 == 0 {
                map.insert(
                    "Field 6",
                    chrono::NaiveDateTime::parse_from_str(
                        "2022-01-01 00:01:45",
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap()
                    .into(),
                );
            }
            map
        });
        let batch_size = pool.get_batch_size();

        let dir_path = env!("TEMP_DIR_PATH");
        std::fs::create_dir_all(dir_path).unwrap();
        let file_name = &std::path::Path::new(dir_path).join("sqlite_raport.csv");
        if file_name.exists() {
            std::fs::remove_file(file_name).unwrap();
        }

        let future = |mut conn| async move {
            super::connection_run(&conn, r"delete from tbl_tst_column_names;", None)?;

            let tx = conn.transaction()?;
            super::batch_run_from_json(&tx, batch_size, sql, param_values).await?;
            tx.commit()?;

            super::download_to_csv(
                &conn,
                r"select * from tbl_tst_column_names;",
                None,
                file_name,
            )
        };
        let res = pool.conn_run(future).await.unwrap();
        assert_eq!(100_000, res, "Download count not as expected");
        assert!(file_name.exists(), "CSV download file not found");

        let mut restricted_columns = HashMap::new();
        restricted_columns.insert(
            "Field 1".to_string(),
            ColumnDefault::Value(GenericWrapper::from("upload test")),
        );
        let future = |mut conn| async move {
            super::connection_run(&conn, r"delete from tbl_tst_column_names;", None)?;
            super::upload_from_text_file(
                &mut conn,
                batch_size,
                "tbl_tst_column_names",
                None,
                Some(&restricted_columns),
                file_name,
                b',',
                Some(b'"'),
                None,
            )
        };
        let res = pool.conn_run(future).await.unwrap();
        std::fs::remove_file(file_name).unwrap();
        assert_eq!(100_000, res, "Upload count not as expected");

        let future = |conn| async move {
            super::connection_get(&conn, "select * from tbl_tst_column_names limit 1;", None)
        };
        let res: R = pool.conn_get(future).await.unwrap();
        assert_eq!(
            &GenericWrapper::from("upload test"),
            res.get(0)
                .unwrap()
                .as_ref()
                .get(&"Field 1".to_string())
                .unwrap(),
            "Returned field value not as expected"
        );
    }

    #[tokio::test]
    async fn xlsx() {
        let pool = get_pool();

        let sql = r#"insert into tbl_tst_column_names ("Field 1", "Field.2", "fiEld 3", "Field_4", "FIELD 5", "Field 6")
            select a.value ->> '$."Field 1"', a.value ->> '$."Field.2"', a.value ->> '$."fiEld 3"', a.value ->> '$."Field_4"', a.value ->> '$."FIELD 5"', a.value ->> '$."Field 6"'
            from json_each(?) as a;
        "#;
        let param_values = futures::stream::iter(0..100_000).map(move |i| {
            let mut map: HashMap<&str, GenericWrapper> = HashMap::new();
            map.insert("Field 1", format!("string {}", i).into());
            map.insert("Field.2", i.into());
            map.insert("fiEld 3", rust_decimal::Decimal::from(i).into());
            if i % 2 == 0 {
                map.insert("Field_4", format!("text {}", i).into());
            }
            map.insert(
                "FIELD 5",
                chrono::NaiveDate::from_ymd_opt(2022, 1, 1).into(),
            );
            if i % 2 == 0 {
                map.insert(
                    "Field 6",
                    chrono::NaiveDateTime::parse_from_str(
                        "2022-01-01 00:01:45",
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap()
                    .into(),
                );
            }
            map
        });
        let batch_size = pool.get_batch_size();

        let dir_path = env!("TEMP_DIR_PATH");
        std::fs::create_dir_all(dir_path).unwrap();
        let file_name = &std::path::Path::new(dir_path).join("sqlite_raport.xlsx");
        if file_name.exists() {
            std::fs::remove_file(file_name).unwrap();
        }

        let future = |mut conn| async move {
            super::connection_run(&conn, r"delete from tbl_tst_column_names;", None)?;

            let tx = conn.transaction()?;
            super::batch_run_from_json(&tx, batch_size, sql, param_values).await?;
            tx.commit()?;

            super::download_to_xlsx(
                &conn,
                r"select * from tbl_tst_column_names;",
                None,
                file_name,
                Some("DATA"),
            )
        };
        let res = pool.conn_run(future).await.unwrap();
        assert_eq!(100_000, res, "Download count not as expected");
        assert!(file_name.exists(), "CSV download file not found");

        let mut restricted_columns = HashMap::new();
        restricted_columns.insert(
            "Field 1".to_string(),
            ColumnDefault::Value(GenericWrapper::from("upload test")),
        );
        let future = |mut conn| async move {
            super::connection_run(&conn, r"delete from tbl_tst_column_names;", None)?;
            super::upload_from_xlsx_file(
                &mut conn,
                batch_size,
                "tbl_tst_column_names",
                None,
                Some(&restricted_columns),
                file_name,
                Some("DATA"),
            )
        };
        let res = pool.conn_run(future).await.unwrap();
        std::fs::remove_file(file_name).unwrap();
        assert_eq!(100_000, res, "Upload count not as expected");

        let future = |conn| async move {
            super::connection_get(&conn, "select * from tbl_tst_column_names limit 1;", None)
        };
        let res: R = pool.conn_get(future).await.unwrap();
        assert_eq!(
            &GenericWrapper::from("upload test"),
            res.get(0)
                .unwrap()
                .as_ref()
                .get(&"Field 1".to_string())
                .unwrap(),
            "Returned field value not as expected"
        );
    }
}
