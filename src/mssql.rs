use crate::{
    error::ErrorReport,
    generics::{ColumnDefault, FieldMetadata, GenericSqlRow, GenericWrapper},
};
use deadpool::managed::{Manager, Object};
use futures::{pin_mut, Future, Stream, StreamExt, TryStreamExt};
use rust_decimal::prelude::ToPrimitive;
use std::{borrow::Cow, collections::HashMap, path::Path, time::Duration};
pub use tiberius::ToSql;
use tiberius::{Client, Config, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use xlsxreader::Reader;

pub type Connection = Object<ConnectionManager>;

pub struct ConnectionManager {
    config: Config,
}

impl ConnectionManager {
    pub fn init(ado_connection_string: String) -> Result<Self, ErrorReport> {
        let config = Config::from_ado_string(ado_connection_string.as_str())?;
        Ok(Self { config })
    }
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for ConnectionManager {
    type Type = Client<Compat<TcpStream>>;
    type Error = ErrorReport;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let tcp = TcpStream::connect(self.config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        let client = Client::connect(self.config.clone(), tcp.compat_write()).await?;
        Ok(client)
    }

    async fn recycle(
        &self,
        client: &mut Self::Type,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        match tokio::time::timeout(
            Duration::from_secs(2),
            client.simple_query("select 1 as res;"),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => Err(deadpool::managed::RecycleError::Backend(err.into())),
            Err(err) => Err(deadpool::managed::RecycleError::Backend(err.into())),
        }
    }
}

pub struct Pool {
    inner: deadpool::managed::Pool<ConnectionManager>,
    batch_size: usize,
}

impl Pool {
    pub fn init(
        ado_connection_string: String,
        max_size: usize,
        batch_size: usize,
    ) -> Result<Self, ErrorReport> {
        let manager = ConnectionManager::init(ado_connection_string)?;
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

    async fn get_session(conn: &mut Object<ConnectionManager>) -> Result<i16, ErrorReport> {
        let row = conn
            .simple_query("select @@SPID as result;")
            .await?
            .into_row()
            .await?;
        let res: Option<i16> = match row {
            Some(r) => r.get(0),
            None => None,
        };

        match res {
            Some(r) => Ok(r),
            None => Err(crate::error::error_sql_no_session_id()),
        }
    }

    pub async fn conn_get<T, F, R>(
        &self,
        callable: F,
        timeout: Duration,
    ) -> Result<Vec<T>, ErrorReport>
    where
        T: TryFrom<tiberius::Row, Error = ErrorReport>,
        F: FnOnce(Connection) -> R,
        R: Future<Output = Result<Vec<T>, ErrorReport>>,
    {
        let mut conn = self
            .inner
            .get()
            .await
            .map_err(|err| crate::error::error_database_connection_pool(err.into()))?;
        let session_id = Self::get_session(&mut conn).await?;

        match tokio::time::timeout(timeout, callable(conn)).await {
            Ok(val) => val,
            Err(_) => {
                let mut conn = self.get_ref().manager().create().await?;
                conn.simple_query(format!("kill {}", session_id)).await?;
                Err(crate::error::error_timeout())
            }
        }
    }

    pub async fn conn_run<F, R>(&self, callable: F, timeout: Duration) -> Result<usize, ErrorReport>
    where
        F: FnOnce(Connection) -> R,
        R: Future<Output = Result<usize, ErrorReport>>,
    {
        let mut conn = self
            .inner
            .get()
            .await
            .map_err(|err| crate::error::error_database_connection_pool(err.into()))?;
        let session_id = Self::get_session(&mut conn).await?;

        match tokio::time::timeout(timeout, callable(conn)).await {
            Ok(val) => val,
            Err(_) => {
                let mut conn = self.get_ref().manager().create().await?;
                conn.simple_query(format!("kill {}", session_id)).await?;
                Err(crate::error::error_timeout())
            }
        }
    }
}

pub async fn connection_get<'a, 'b: 'a, R>(
    conn: &'a mut Object<ConnectionManager>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
) -> Result<Vec<R>, ErrorReport>
where
    R: TryFrom<tiberius::Row, Error = ErrorReport>,
{
    let params = if let Some(p) = param_values { p } else { &[] };
    let rowstream = conn.query(query_string, params).await?;

    let mut res: Vec<R> = Vec::new();
    pin_mut!(rowstream);
    while let Some(item) = rowstream.try_next().await? {
        if let QueryItem::Row(r) = item {
            res.push(r.try_into()?)
        }
    }
    Ok(res)
}

pub async fn connection_run<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
) -> Result<usize, ErrorReport> {
    let params = if let Some(p) = param_values { p } else { &[] };
    let res = conn.execute(query_string, params).await?;
    Ok(res.total() as usize)
}

pub async fn batch_get<'a, 'b: 'a, R>(
    conn: &'a mut Object<ConnectionManager>,
    query_string: &'a str,
    param_values: impl Iterator<Item = Vec<Box<dyn ToSql>>>,
) -> Result<Vec<R>, ErrorReport>
where
    R: TryFrom<tiberius::Row, Error = ErrorReport>,
{
    conn.simple_query("BEGIN TRAN").await?;

    let mut res = Vec::new();
    for p in param_values {
        let params: Vec<_> = p.iter().map(|v| v.as_ref()).collect();
        let rowstream = conn.query(query_string, params.as_slice()).await?;
        pin_mut!(rowstream);
        while let Some(item) = rowstream.try_next().await? {
            if let QueryItem::Row(r) = item {
                res.push(r.try_into()?)
            }
        }
    }
    conn.simple_query("COMMIT").await?;
    Ok(res)
}

pub async fn batch_run<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    query_string: &'a str,
    param_values: impl Iterator<Item = Vec<Box<dyn ToSql>>>,
) -> Result<usize, ErrorReport> {
    conn.simple_query("BEGIN TRAN").await?;

    let mut res: u64 = 0;
    for p in param_values {
        let params: Vec<_> = p.iter().map(|v| v.as_ref()).collect();
        let response = conn.execute(query_string, params.as_slice()).await?;
        res += response.total();
    }
    conn.simple_query("COMMIT").await?;
    Ok(res as usize)
}

pub async fn batch_run_from_json<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    batch_size: usize,
    query_string: &'a str,
    param_values: impl Stream<Item = impl serde::Serialize + 'b>,
) -> Result<usize, ErrorReport> {
    conn.simple_query("BEGIN TRAN").await?;

    let mut res: u64 = 0;
    let mut contor: usize = 0;
    let mut recs = Vec::new();
    pin_mut!(param_values);
    while let Some(vals) = param_values.next().await {
        recs.push(vals);
        contor += 1;

        if contor % batch_size != 0 {
            continue;
        }

        let json = serde_json::json!(&recs).to_string();
        let p: &dyn ToSql = &Cow::from(&json);
        let response = conn.execute(query_string, &[p]).await.map_err(|err| {
            crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
        })?;
        res += response.total();
        recs.clear();
    }
    if !recs.is_empty() {
        let json = serde_json::json!(&recs).to_string();
        let p: &dyn ToSql = &Cow::from(&json);
        let response = conn.execute(query_string, &[p]).await.map_err(|err| {
            crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
        })?;
        res += response.total();
        recs.clear();
    }
    conn.simple_query("COMMIT").await?;
    Ok(res as usize)
}

/* pub async fn bulk_insert<'a, 'b: 'a, T>(
    conn: &'a mut Object<MSSQLConnectionManager>,
    table_schema: &'a str,
    table_name: &'a str,
    param_values: impl Stream<Item = T>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>
where
    T: tiberius::IntoRow<'b>,
{
    let table = format!("[{}].[{}]", table_schema, table_name);
    let mut tx = conn.bulk_insert(table.as_str()).await?;

    pin_mut!(param_values);
    while let Some(vals) = param_values.next().await {
        tx.send(vals.into_row()).await?;
    }
    let res = tx.finalize().await?;
    Ok(res.total() as usize)
} */

async fn table_meta(
    conn: &mut Object<ConnectionManager>,
    db_schema: &str,
    table_name: &str,
) -> Result<Vec<FieldMetadata>, ErrorReport> {
    let sql = r#"
        select
            a.column_name,
            a.character_maximum_length,
            a.numeric_precision,
            a.numeric_precision_radix,
            a.numeric_scale,
            a.data_type
        from information_schema.columns as a
        where concat(a.table_schema, '.', a.table_name) = @P1;
    "#;

    let rowstream = conn
        .query(sql, &[&format!("{}.{}", db_schema, table_name)])
        .await?;
    pin_mut!(rowstream);

    let mut res = Vec::new();
    while let Some(item) = rowstream.try_next().await? {
        if let QueryItem::Row(r) = item {
            res.push(r.try_into()?)
        }
    }
    if res.is_empty() {
        return Err(crate::error::error_no_table_meta(&db_schema, &table_name));
    }
    Ok(res)
}

fn _upload_sql_string<'a, 'b: 'a>(
    db_schema: &'a str,
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
        .map(|val| format!(r"[{}]", val.name()))
        .collect::<Vec<_>>()
        .join(",");

    let ins_col_list_rstr = {
        let res = cols_rstr
            .iter()
            .filter(|(_, v)| !matches!(v, ColumnDefault::ByDatabase))
            .map(|(k, _)| format!(r"[{}]", k))
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
        .map(|val| format!(r"a.[{}]", val.name()))
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
                    format!(r"a.[{}]", k)
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

    let json_col_list_alwd = cols_alwd
        .iter()
        .map(|val| format!(r"[{}] {}", val.name(), val.mssql_type_name()))
        .collect::<Vec<_>>()
        .join(",");

    let json_col_list_rstr = {
        let res = cols_rstr
            .iter()
            .filter(|(_, v)| matches!(v, ColumnDefault::Value(_)))
            .filter_map(|(k, _)| table_meta.get(k))
            .map(|v| format!(r"[{}] {}", v.name(), v.mssql_type_name()))
            .collect::<Vec<_>>()
            .join(",");

        if res.is_empty() {
            res
        } else {
            format!(",{}", res)
        }
    };

    let res = if let Some(constr_cols) = upsert_constraint_columns {
        let constr_text = constr_cols
            .iter()
            .map(|val| format!("t.[{}] = s.[{}]", val, val))
            .collect::<Vec<_>>()
            .join(" and ");

        let excl_cols = cols_rstr
            .iter()
            .filter(|(_, v)| matches!(v, ColumnDefault::ByDatabase))
            .map(|(k, _)| k.to_owned())
            .collect::<Vec<_>>();

        let upd_col_list_alwd = cols_alwd
            .iter()
            .filter(|v| !constr_cols.contains(&v.name().as_str()) && !excl_cols.contains(&v.name()))
            .map(|v| format!(r"[{}] = s.[{}]", v.name(), v.name()))
            .collect::<Vec<_>>()
            .join(",");

        let upd_col_list_rstr = {
            let res = cols_rstr
                .iter()
                .filter(|(k, _)| !constr_cols.contains(&k.as_str()) && !excl_cols.contains(k))
                .map(|(k, v)| match v {
                    ColumnDefault::Value(_) => format!(r"[{}] = s.[{}]", k, k),
                    ColumnDefault::Formula(f) => format!(r"[{}] = {}", k, f),
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
            r"
            merge into [{}].[{}] as t
            using (select {}{} from openjson(@P1) with ({}{}) as a) as s
            on ({})
            when matched then update set {}{}
            when not matched by target then insert ({}{}) values ({}{});
        ",
            db_schema,
            table_name,
            sel_col_list_alwd,
            sel_col_list_rstr,
            json_col_list_alwd,
            json_col_list_rstr,
            constr_text,
            upd_col_list_alwd,
            upd_col_list_rstr,
            ins_col_list_alwd,
            ins_col_list_rstr,
            sel_col_list_alwd,
            sel_col_list_rstr
        )
    } else {
        format!(
            r"insert into [{}].[{}] ({}{}) select {}{} from openjson(@P1) with ({}{}) as a;",
            db_schema,
            table_name,
            ins_col_list_alwd,
            ins_col_list_rstr,
            sel_col_list_alwd,
            sel_col_list_rstr,
            json_col_list_alwd,
            json_col_list_rstr
        )
    };
    Ok(res)
}

pub async fn upload_from_text_file<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    batch_size: usize,
    db_schema: &'a str,
    table_name: &'a str,
    upsert_constraint_columns: Option<&'b [&'b str]>,
    restricted_columns: Option<&'b HashMap<String, ColumnDefault<'b, GenericWrapper>>>,
    file_path: &'b Path,
    file_column_delimiter: u8,
    file_column_quote_char: Option<u8>,
    file_quote_char_escape: Option<u8>,
) -> Result<usize, ErrorReport> {
    let metadata = table_meta(conn, db_schema, table_name)
        .await?
        .into_iter()
        .map(|v| (v.name(), v))
        .collect();

    let sql = _upload_sql_string(
        db_schema,
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

    conn.simple_query("BEGIN TRAN").await?;

    let mut res: u64 = 0;
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

        let json = serde_json::json!(&recs).to_string();
        let p: &dyn ToSql = &Cow::from(&json);
        let response = conn.execute(&sql, &[p]).await.map_err(|err| {
            crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
        })?;
        res += response.total();
        recs.clear();
    }

    if !recs.is_empty() {
        let json = serde_json::json!(&recs).to_string();
        let p: &dyn ToSql = &Cow::from(&json);
        let response = conn.execute(&sql, &[p]).await.map_err(|err| {
            crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
        })?;
        res += response.total();
        recs.clear();
    }

    conn.simple_query("COMMIT").await?;
    Ok(res as usize)
}

pub async fn download_to_csv<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
    file_path: &'b Path,
) -> Result<usize, ErrorReport> {
    let params = if let Some(p) = param_values { p } else { &[] };
    let rowstream = conn.query(query_string, params).await?;

    let mut writer = csv::Writer::from_path(file_path)?;

    let mut res: usize = 0;
    pin_mut!(rowstream);
    while let Some(item) = rowstream.try_next().await? {
        let rec: GenericSqlRow<String, GenericWrapper> = if let QueryItem::Row(r) = item {
            r.try_into()?
        } else {
            continue;
        };
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

pub async fn upload_from_xlsx_file<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    batch_size: usize,
    db_schema: &'a str,
    table_name: &'a str,
    upsert_constraint_columns: Option<&'b [&'b str]>,
    restricted_columns: Option<&'b HashMap<String, ColumnDefault<'b, GenericWrapper>>>,
    file_path: &'b Path,
    sheet_name: Option<&'a str>,
) -> Result<usize, ErrorReport> {
    let metadata = table_meta(conn, db_schema, table_name)
        .await?
        .into_iter()
        .map(|v| (v.name(), v))
        .collect();

    let sql = _upload_sql_string(
        db_schema,
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

    conn.simple_query("BEGIN TRAN").await?;

    let mut res: u64 = 0;
    let mut contor: usize = 0;
    let mut recs: Vec<HashMap<String, GenericWrapper>> = Vec::new();
    for rx in start_row + 1..=end_row {
        let mut record: HashMap<String, GenericWrapper> = HashMap::new();
        for cx in start_col..=end_col {
            let Some(col_name) = header.get(usize::try_from(cx)?) else {
                return Err(crate::error::error_xlsx_column_name(&cx));
            };
            let col_type = if let Some(f) = metadata.get(col_name) {
                f.mssql_type_name().to_lowercase().trim().replace(' ', "")
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
            } else if col_type.eq("datetime")
                || col_type.eq("datetime2")
                || col_type.eq("smalldatetime")
            {
                if let Some(p) = v.as_datetime() {
                    GenericWrapper::NaiveTimestamp(p)
                } else {
                    crate::helper::naive_xlsx_cast(v, rx, cx)?
                }
            } else if col_type.starts_with("char")
                || col_type.starts_with("ntext")
                || col_type.starts_with("varchar")
                || col_type.starts_with("nvarchar")
                || col_type.starts_with("xml")
            {
                crate::helper::naive_xlsx_cast(v, rx, cx)?
            } else if col_type.starts_with("float")
                || col_type.starts_with("numeric")
                || col_type.starts_with("decimal")
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
            } else if col_type.eq("uniqueidentifier") {
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

        let json = serde_json::json!(&recs).to_string();
        let p: &dyn ToSql = &Cow::from(&json);
        let response = conn.execute(&sql, &[p]).await.map_err(|err| {
            crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
        })?;
        res += response.total();
        recs.clear();
    }
    if !recs.is_empty() {
        let json = serde_json::json!(&recs).to_string();
        let p: &dyn ToSql = &Cow::from(&json);
        let response = conn.execute(&sql, &[p]).await.map_err(|err| {
            crate::error::error_sql_batch_load(&(res + 1), &contor, Box::new(err))
        })?;
        res += response.total();
        recs.clear();
    }

    conn.simple_query("COMMIT").await?;
    Ok(res as usize)
}

pub async fn download_to_xlsx<'a, 'b: 'a>(
    conn: &'a mut Object<ConnectionManager>,
    query_string: &'a str,
    param_values: Option<&'b [&'b dyn ToSql]>,
    file_path: &'b Path,
    sheet_name: Option<&'a str>,
) -> Result<usize, ErrorReport> {
    let params = if let Some(p) = param_values { p } else { &[] };
    let rowstream = conn.query(query_string, params).await?;

    let mut workbook = xlsxwriter::Workbook::new();
    let worksheet = workbook.add_worksheet();
    let sh_name = if let Some(v) = sheet_name {
        v
    } else {
        "Sheet1"
    };
    worksheet.set_name(sh_name)?;
    let formats = crate::helper::XlsxFormats::new();

    pin_mut!(rowstream);
    let mut res: usize = 0;
    let mut col_types: Vec<tiberius::ColumnType> = Vec::new();
    while let Some(item) = rowstream.try_next().await? {
        let row = if let QueryItem::Row(r) = item {
            r
        } else {
            continue;
        };
        if res == 0 {
            col_types = row.columns().iter().map(|c| c.column_type()).collect();
            for i in 0..col_types.len() {
                worksheet.write_string(
                    0,
                    u16::try_from(i)?,
                    row.columns()[i].name(),
                    &formats.bold,
                )?;
            }
        }
        for i in 0..col_types.len() {
            let Some(ty) = col_types.get(i) else {
                return Err(crate::error::error_xlsx_column_sql_type(&i));
            };
            match *ty {
                tiberius::ColumnType::Null => {}
                tiberius::ColumnType::Bit | tiberius::ColumnType::Bitn => {
                    if let Option::<bool>::Some(v) = row.try_get(i)? {
                        worksheet.write_boolean_only(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            v,
                        )?;
                    }
                }
                tiberius::ColumnType::BigChar
                | tiberius::ColumnType::NVarchar
                | tiberius::ColumnType::BigVarChar
                | tiberius::ColumnType::NChar
                | tiberius::ColumnType::Xml => {
                    if let Option::<&str>::Some(v) = row.try_get(i)? {
                        worksheet.write_string_only(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            v,
                        )?;
                    }
                }
                tiberius::ColumnType::Guid => {
                    if let Option::<uuid::Uuid>::Some(v) = row.try_get(i)? {
                        worksheet.write_string_only(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            v.to_string().as_str(),
                        )?;
                    }
                }
                tiberius::ColumnType::Int1 | tiberius::ColumnType::Int2 => {
                    if let Option::<i16>::Some(v) = row.try_get(i)? {
                        worksheet.write_number(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            v,
                            &formats.integer,
                        )?;
                    } else {
                        worksheet.write_blank(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            &formats.integer,
                        )?;
                    }
                }
                tiberius::ColumnType::Int4 => {
                    if let Option::<i32>::Some(v) = row.try_get(i)? {
                        worksheet.write_number(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            v,
                            &formats.integer,
                        )?;
                    } else {
                        worksheet.write_blank(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            &formats.integer,
                        )?;
                    }
                }
                tiberius::ColumnType::Int8 | tiberius::ColumnType::Intn => {
                    if let Option::<i64>::Some(v) = row.try_get(i)? {
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
                }
                tiberius::ColumnType::Float4 => {
                    if let Option::<f32>::Some(v) = row.try_get(i)? {
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
                }
                tiberius::ColumnType::Float8 | tiberius::ColumnType::Floatn => {
                    if let Option::<f64>::Some(v) = row.try_get(i)? {
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
                }
                tiberius::ColumnType::Decimaln | tiberius::ColumnType::Numericn => {
                    if let Option::<rust_decimal::Decimal>::Some(v) = row.try_get(i)? {
                        if let Some(p) = v.to_f64() {
                            worksheet.write_number(
                                u32::try_from(res + 1)?,
                                u16::try_from(i)?,
                                p,
                                &formats.decimal,
                            )?;
                        } else {
                            worksheet.write_string(
                                u32::try_from(res + 1)?,
                                u16::try_from(i)?,
                                v.to_string().as_str(),
                                &formats.decimal,
                            )?;
                        }
                    } else {
                        worksheet.write_blank(
                            u32::try_from(res + 1)?,
                            u16::try_from(i)?,
                            &formats.decimal,
                        )?;
                    }
                }
                tiberius::ColumnType::Daten => {
                    if let Option::<chrono::NaiveDate>::Some(v) = row.try_get(i)? {
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
                }
                tiberius::ColumnType::Timen => {
                    if let Option::<chrono::NaiveTime>::Some(v) = row.try_get(i)? {
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
                }
                tiberius::ColumnType::Datetime
                | tiberius::ColumnType::Datetime2
                | tiberius::ColumnType::Datetime4
                | tiberius::ColumnType::Datetimen => {
                    if let Option::<chrono::NaiveDateTime>::Some(v) = row.try_get(i)? {
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
                }
                _ => {
                    return Err(crate::error::error_sql_to_xlsx_value(
                        &format!("{ty:?}"),
                        &row.columns()[i].name(),
                    ))
                }
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
    use std::{collections::HashMap, path::Path, time::Duration};
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    use utils;

    type R = Vec<GenericSqlRow<String, GenericWrapper>>;

    #[tokio::test]
    async fn connection() {
        let app_config_path = env!("APP_CONFIG_FILE_PATH");
        let app_config = utils::envars::AppConfig::init(
            Path::new(app_config_path),
            utils::envars::CONFIG_FILE_DELIMITER,
        )
        .unwrap();
        let config =
            tiberius::Config::from_ado_string(app_config.get_var("MSSQL:CONN_STRING").unwrap())
                .unwrap();
        let tcp = tokio::net::TcpStream::connect(config.get_addr())
            .await
            .unwrap();
        tcp.set_nodelay(true).unwrap();
        let mut client = tiberius::Client::connect(config, tcp.compat_write())
            .await
            .unwrap();
        let query = client.query("SELECT 1", &[]).await.unwrap();
        let row = query.into_row().await.unwrap().unwrap();

        let value: i32 = row.get(0).unwrap();
        assert_eq!(1, value);
    }

    #[tokio::test]
    async fn pool() {
        let config_path = env!("APP_CONFIG_FILE_PATH");
        let config = utils::envars::AppConfig::init(
            Path::new(config_path),
            utils::envars::CONFIG_FILE_DELIMITER,
        )
        .unwrap();
        let pool = super::Pool::init(
            config.get_var("MSSQL:CONN_STRING").unwrap().to_owned(),
            5,
            3000,
        )
        .unwrap();

        for _ in 0..5 {
            let mut conn = pool.get_ref().get().await.unwrap();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let _ = conn.simple_query("select 1 as res").await;
            });
        }

        let pool_size = pool.get_ref().status().size;
        assert_eq!(5, pool_size, "Pool not of configured size");
    }

    fn get_pool() -> super::Pool {
        let config_path = env!("APP_CONFIG_FILE_PATH");
        let config = utils::envars::AppConfig::init(
            Path::new(config_path),
            utils::envars::CONFIG_FILE_DELIMITER,
        )
        .unwrap();
        super::Pool::init(
            config.get_var("MSSQL:CONN_STRING").unwrap().to_owned(),
            1,
            3000,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn query_result() {
        let pool = get_pool();

        let param_values: &[&dyn super::ToSql] = &[
            &GenericWrapper::from(1),
            &GenericWrapper::from("primul".to_owned()),
            &GenericWrapper::from(chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()),
        ];

        let future = |mut conn| async move {
            super::connection_get(
                &mut conn,
                r#"select @P1 as id, @P2 as [text], @P3 as [date];"#,
                Some(param_values),
            )
            .await
        };

        let recs: R = pool
            .conn_get(future, Duration::from_secs(10))
            .await
            .unwrap();
        let rec = recs.get(0).unwrap();
        assert_eq!(
            (
                &GenericWrapper::I32(1_i32),
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

        let param_values: &[&dyn super::ToSql] = &[
            &GenericWrapper::I32(2),
            &GenericWrapper::None,
            &GenericWrapper::None,
        ];

        let future = |mut conn| async move {
            super::connection_get(
                &mut conn,
                r#"select @P1 as id, @P2 as [text], @P3 as [date];"#,
                Some(param_values),
            )
            .await
        };

        let recs: R = pool
            .conn_get(future, Duration::from_secs(10))
            .await
            .unwrap();
        let rec = recs.get(0).unwrap();
        assert_eq!(
            (
                &GenericWrapper::I32(2_i32),
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

        let future = |mut conn| async move {
            super::connection_get(
                &mut conn,
                r#"select 3 as id, 'al treilea' as [text], cast('2022-01-03' as date) as [date] where 1 = 2;"#,
                None,
            )
            .await
        };

        let recs: R = pool
            .conn_get(future, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(
            0,
            recs.len(),
            "Query result should have contained no record"
        );

        let param_values: &[&dyn super::ToSql] = &[
            &GenericWrapper::I32(4),
            &GenericWrapper::String("al patrulea".to_owned()),
            &GenericWrapper::NaiveDate(chrono::NaiveDate::from_ymd_opt(2022, 1, 4).unwrap()),
        ];

        let future = |mut conn| async move {
            super::connection_get(
                &mut conn,
                r#"select @P1 as id, @P2 as [text], @P3 as [date] into #tbl_tmp_tabela;"#,
                Some(param_values),
            )
            .await
        };

        let recs: R = pool
            .conn_get(future, Duration::from_secs(10))
            .await
            .unwrap();
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
            &GenericWrapper::I32(1),
            &GenericWrapper::String("primul".to_owned()),
            &GenericWrapper::NaiveDate(chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()),
        ];

        let future = |mut conn| async move {
            let res = super::connection_run(
                &mut conn,
                r#"
                    select
                        id, label
                    into #tbl_tmp_tabela
                    from (
                        select 1 as id, 'string1' as label union
                        select 2 as id, 'string2' as label union
                        select 3 as id, 'string3' as label union
                        select 4 as id, 'string4' as label
                    ) as a;
                "#,
                None,
            )
            .await?;

            super::connection_run(
                &mut conn,
                r#"select @P1 as id, @P2 as [text], @P3 as [date];"#,
                Some(param_values),
            )
            .await
            .map(|v| v + res)
        };

        let res = pool
            .conn_run(future, std::time::Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(5, res, "Execute count is not as expected");
    }

    #[tokio::test]
    async fn query_timeout() {
        let pool = get_pool();

        let future = |mut conn| async move {
            super::connection_run(
                &mut conn,
                r"
                    begin
                        waitfor delay '00:00:59';
                        select 1 as result;
                    end;
                ",
                None,
            )
            .await
        };

        let res = pool.conn_run(future, Duration::from_secs(3)).await;
        assert_eq!(
            crate::error::error_timeout().to_string(),
            res.err().unwrap().to_string(),
            "Not the correct error"
        );

        let future = |mut conn| async move {
            super::connection_get(
                &mut conn,
                r#"select 3 as id, 'al treilea' as [text], cast('2022-01-03' as date) as [date];"#,
                None,
            )
            .await
        };

        let res: R = pool
            .conn_get(future, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(
            1,
            res.len(),
            "Query did not return the corect number of records"
        );
    }

    #[tokio::test]
    async fn transaction_execute() {
        let pool = get_pool();

        let param_values_1: &[&dyn super::ToSql] = &[
            &GenericWrapper::None,
            &GenericWrapper::from("primul"),
            &GenericWrapper::from(chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()),
        ];

        let param_values_2: &[&dyn super::ToSql] = &[
            &GenericWrapper::from(2),
            &GenericWrapper::from("al doilea"),
            &GenericWrapper::None,
        ];

        let param_values_3 = param_values_2.clone();

        let future = |mut conn: super::Connection| async move {
            conn.simple_query("BEGIN TRAN").await?;
            conn.simple_query(r#"drop table if exists #tbl_tmp_tabela;"#)
                .await?;
            conn.simple_query(
                r#"create table #tbl_tmp_tabela (id int, [text] nvarchar(300), [date] date);"#,
            )
            .await?;
            let mut res = super::connection_run(
                &mut conn,
                r#"insert into #tbl_tmp_tabela (id, [text], [date]) values (@P1, @P2, @P3);"#,
                Some(param_values_1),
            )
            .await?;
            res += super::connection_run(
                &mut conn,
                r#"insert into #tbl_tmp_tabela (id, [text], [date]) values (@P1, @P2, @P3);"#,
                Some(param_values_2),
            )
            .await?;
            conn.simple_query("COMMIT").await?;
            Ok(res)
        };

        let res = pool
            .conn_run(future, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(
            2, res,
            "Transaction did not return the correct affected records number"
        );

        let future = |mut conn: super::Connection| async move {
            conn.simple_query("BEGIN TRAN").await?;
            conn.simple_query(r#"drop table if exists #tbl_tmp_tabela;"#)
                .await?;
            conn.simple_query(
                r#"create table #tbl_tmp_tabela (id int, [text] text, [date] date, constraint tbl_tmp_tabela_ck1 check (id <= 1));"#,
            )
            .await?;
            let res = super::connection_run(
                &mut conn,
                r#"insert into #tbl_tmp_tabela (id, [text], [date]) values (@P1, @P2, @P3);"#,
                Some(param_values_3),
            )
            .await?;
            conn.simple_query("COMMIT").await?;
            Ok(res)
        };

        let res = pool
            .conn_run(future, std::time::Duration::from_secs(10))
            .await;
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
        let sql = r#"
            insert into test.tbl_tst_values (field1, field2, field3, field4, field5, field6)
            output inserted.id
            values (@P1, @P2, @P3, @P4, @P5, @P6);
        "#;
        let mut contor = 0;
        let param_values = std::iter::from_fn(|| {
            contor += 1;
            if contor > 4700 {
                return None;
            }

            let res: Vec<Box<dyn super::ToSql>> = vec![
                Box::new(format!("text {}", contor)),
                Box::new(contor),
                Box::new(rust_decimal::Decimal::from(contor)),
                Box::new(format!("string {}", contor)),
                Box::new(chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()),
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

        let future = |mut conn| async move {
            let res: R = super::batch_get(&mut conn, sql, param_values).await?;
            Ok(res)
        };

        let recs = pool
            .conn_get(future, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(4700, recs.len(), "Result count is not as expected");
        if let GenericWrapper::Uuid(_) = recs
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

        let sql = r#"delete from test.tbl_tst_values where id = @P1;"#;
        let param_values = recs.iter().map(|x| {
            let res: Box<dyn super::ToSql> =
                Box::new(x.as_ref().get(&"id".to_string()).unwrap().clone());
            vec![res]
        });

        let future = |mut conn| async move {
            let res = super::batch_run(&mut conn, sql, param_values).await?;
            Ok(res)
        };
        let res = pool
            .conn_run(future, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(4700, res, "Deleted position count is not as expected");
    }

    #[tokio::test]
    async fn stream_processing() {
        let pool = get_pool();

        let sql = r#"
            insert into test.tbl_tst_values (field1, field2, field3, field4, field5, field6)
            select a.field1, a.field2, a.field3, a.field4, a.field5, a.field6
            from openjson(@P1) with (
                field1 nvarchar(300),
                field2 int,
                field3 decimal(20,5),
                field4 nvarchar(300),
                field5 date,
                field6 datetime
            ) as a;
        "#;

        let param_values = futures::stream::iter(0..100_000).map(move |i| {
            let mut map: HashMap<&str, GenericWrapper> = HashMap::new();
            map.insert("field1", "stream testing".into());
            map.insert("field2", i.into());
            map.insert("field3", rust_decimal::Decimal::from(i).into());
            map.insert("field4", format!("text {}", i).into());
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

        let future = |mut conn| async move {
            let res = super::batch_run_from_json(&mut conn, batch_size, sql, param_values).await?;
            Ok(res)
        };
        let res = pool
            .conn_run(future, Duration::from_secs(120))
            .await
            .unwrap();
        assert_eq!(100_000, res, "Insert count not as expected");

        let sql = r#"delete from test.tbl_tst_values where field1 = 'stream testing'"#;
        let future = |mut conn| async move { super::connection_run(&mut conn, sql, None).await };
        let res = pool
            .conn_run(future, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(100_000, res, "Delete count not as expected");
    }

    /* #[tokio::test]
    async fn bulk_insert() {
        let pool = get_pool();

        let param_values = futures::stream::iter(0..100_000).map(move |i| {
            let mut map = MyMap::new();
            map.insert("id", GenericWrapper::Uuid(uuid::Uuid::new_v4()));
            map.insert("field1", GenericWrapper::from("stream testing"));
            map.insert("field2", GenericWrapper::I32(i));
            map.insert(
                "field3",
                GenericWrapper::from(rust_decimal::Decimal::new(i as i64 * 10_i64.pow(5), 5)),
            );
            map.insert("field4", GenericWrapper::from(format!("text {}", i)));
            map.insert(
                "field5",
                GenericWrapper::from(chrono::NaiveDate::from_ymd(2022, 1, 1)),
            );
            map.insert(
                "field6",
                GenericWrapper::from(
                    chrono::NaiveDateTime::parse_from_str(
                        "2022-01-01 00:01:45",
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap(),
                ),
            );
            map
        });

        let future = |conn: super::MSSQLSyncConnection| async move {
            let mut lock = conn.try_lock().map_err(|e| e.to_string())?;
            let _conn = lock.deref_mut();

            let res = super::bulk_insert(_conn, "test", "tbl_tst_values", param_values).await?;
            Ok(res)
        };
        let res = pool
            .conn_run(future, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(100_000, res, "Insert count not as expected");

        let sql = r#"delete from test.tbl_tst_values where field1 = 'stream testing'"#;
        let future = |conn: super::MSSQLSyncConnection| async move {
            let mut lock = conn.try_lock().map_err(|e| e.to_string())?;
            let conn = lock.deref_mut();

            super::connection_run(conn, sql, None).await
        };
        let res = pool
            .conn_run(future, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(100_000, res, "Delete count not as expected");
    } */

    #[tokio::test]
    async fn table_meta() {
        let pool = get_pool();

        let future =
            |mut conn| async move { super::table_meta(&mut conn, "test", "tbl_tst_users").await };

        let res = pool
            .conn_get(future, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(
            !res.is_empty(),
            "Did not get any column metadata for table 'test.tbl_tst_users"
        );

        let table_meta: HashMap<_, _> = res.into_iter().map(|v| (v.name(), v)).collect();

        let mut restricted_columns: HashMap<String, ColumnDefault<GenericWrapper>> = HashMap::new();
        restricted_columns.insert("id".to_string(), ColumnDefault::ByDatabase);
        restricted_columns.insert(
            "mod_de".to_string(),
            ColumnDefault::Value(GenericWrapper::from("C12153")),
        );
        restricted_columns.insert(
            "mod_timp".to_string(),
            ColumnDefault::Formula("current_timestamp"),
        );

        let sql = super::_upload_sql_string(
            "test",
            "tbl_tst_users",
            &table_meta,
            Some(&restricted_columns),
            Some(&["username"]),
        )
        .unwrap();
        assert!(
            sql.contains(r"[mod_de] = s.[mod_de]"),
            "The resulted sql statement is illformed"
        );
    }

    #[tokio::test]
    async fn csv() {
        let pool = get_pool();

        let sql = r"insert into test.tbl_tst_column_names ([Field 1], [Field.2], [fiEld 3], [Field_4], [FIELD 5], [Field 6])
            select a.[Field 1], a.[Field.2], a.[fiEld 3], a.[Field_4], a.[FIELD 5], a.[Field 6]
            from openjson(@P1) with (
                [Field 1] nvarchar(1000),
                [Field.2] int,
                [fiEld 3] decimal(10,5),
                [Field_4] nvarchar(1000),
                [FIELD 5] date,
                [Field 6] datetime
            ) as a;";
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
        let file_name = &std::path::Path::new(dir_path).join("mssql_raport.csv");
        if file_name.exists() {
            std::fs::remove_file(file_name).unwrap();
        }

        let future = |mut conn: super::Connection| async move {
            conn.simple_query(r"truncate table test.tbl_tst_column_names;")
                .await?;
            super::batch_run_from_json(&mut conn, batch_size, sql, param_values).await?;
            super::download_to_csv(
                &mut conn,
                r"select * from test.tbl_tst_column_names;",
                None,
                file_name,
            )
            .await
        };

        let res = pool
            .conn_run(future, Duration::from_secs(150))
            .await
            .unwrap();
        assert_eq!(100_000, res, "Download count not as expected");
        assert!(file_name.exists(), "CSV download file not found");

        let mut restricted_columns = HashMap::new();
        restricted_columns.insert(
            "Field 1".to_string(),
            ColumnDefault::Value(GenericWrapper::from("upload test")),
        );

        let future = |mut conn: super::Connection| async move {
            conn.simple_query(r"truncate table test.tbl_tst_column_names;")
                .await?;
            super::upload_from_text_file(
                &mut conn,
                batch_size,
                "test",
                "tbl_tst_column_names",
                None,
                Some(&restricted_columns),
                file_name,
                b',',
                Some(b'"'),
                None,
            )
            .await
        };

        let res = pool
            .conn_run(future, Duration::from_secs(120))
            .await
            .unwrap();
        std::fs::remove_file(file_name).unwrap();
        assert_eq!(100_000, res, "Upload count not as expected");

        let future = |mut conn| async move {
            super::connection_get(
                &mut conn,
                "select top 1 * from test.tbl_tst_column_names;",
                None,
            )
            .await
        };

        let res: R = pool
            .conn_get(future, Duration::from_secs(10))
            .await
            .unwrap();
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

        let sql = r"insert into test.tbl_tst_column_names ([Field 1], [Field.2], [fiEld 3], [Field_4], [FIELD 5], [Field 6])
            select a.[Field 1], a.[Field.2], a.[fiEld 3], a.[Field_4], a.[FIELD 5], a.[Field 6]
            from openjson(@P1) with (
                [Field 1] nvarchar(1000),
                [Field.2] int,
                [fiEld 3] decimal(10,5),
                [Field_4] nvarchar(1000),
                [FIELD 5] date,
                [Field 6] datetime
            ) as a;";
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
        let file_name = &std::path::Path::new(dir_path).join("mssql_raport.xlsx");
        if file_name.exists() {
            std::fs::remove_file(file_name).unwrap();
        }

        let future = |mut conn: super::Connection| async move {
            conn.simple_query(r"truncate table test.tbl_tst_column_names;")
                .await?;
            super::batch_run_from_json(&mut conn, batch_size, sql, param_values).await?;
            super::download_to_xlsx(
                &mut conn,
                r"select * from test.tbl_tst_column_names;",
                None,
                file_name,
                Some("DATA"),
            )
            .await
        };

        let res = pool
            .conn_run(future, Duration::from_secs(150))
            .await
            .unwrap();
        assert_eq!(100_000, res, "Download count not as expected");
        assert!(file_name.exists(), "XLSX download file not found");

        let mut restricted_columns = HashMap::new();
        restricted_columns.insert(
            "Field 1".to_string(),
            ColumnDefault::Value(GenericWrapper::from("upload test")),
        );

        let future = |mut conn: super::Connection| async move {
            conn.simple_query(r"truncate table test.tbl_tst_column_names;")
                .await?;
            super::upload_from_xlsx_file(
                &mut conn,
                batch_size,
                "test",
                "tbl_tst_column_names",
                None,
                Some(&restricted_columns),
                file_name,
                Some("DATA"),
            )
            .await
        };

        let res = pool
            .conn_run(future, Duration::from_secs(120))
            .await
            .unwrap();
        std::fs::remove_file(file_name).unwrap();
        assert_eq!(100_000, res, "Upload count not as expected");

        let future = |mut conn| async move {
            super::connection_get(
                &mut conn,
                "select top 1 * from test.tbl_tst_column_names;",
                None,
            )
            .await
        };

        let res: R = pool
            .conn_get(future, Duration::from_secs(10))
            .await
            .unwrap();
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
