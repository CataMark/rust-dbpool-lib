use thiserror::Error;

#[derive(Error, Debug)]
pub enum ErrorReport {
    #[error("{:?} - {0}", .0.kind())]
    Io(#[from] std::io::Error),
    #[error("FromUtf8 - {0}")]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error("REGEX - {0}")]
    Regex(#[from] regex::Error),
    #[error("UUID - {0}")]
    Uuid(#[from] uuid::Error),
    #[error("ChronoParse - {0}")]
    ChronoParse(#[from] chrono::ParseError),
    #[error("SerdeJson - {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("RustDecimal - {0}")]
    RustDecimal(#[from] rust_decimal::Error),
    #[error("OpenSslErrorStack - {0}")]
    OpenSslErrorStack(#[from] openssl::error::ErrorStack),
    #[error("TokioTimeElapsed - {0}")]
    TokioTimeElapsed(#[from] tokio::time::error::Elapsed),
    #[error("CSV - {0}")]
    Csv(#[from] csv::Error),
    #[error("CsvReadRow - error reading row {}. Original error: {}", .row, .source)]
    CsvReadRow {
        source: Box<dyn std::error::Error + Send + Sync>,
        row: String,
    },
    #[error("XlsxReader - {0}")]
    XlsxReader(#[from] xlsxreader::XlsxError),
    #[error("XlsxWriter - {0}")]
    XlsxWriter(#[from] xlsxwriter::XlsxError),
    #[error("error found at row {} and column {}", row, col)]
    XlsxCellValue { row: String, col: String },
    #[error("XlsxNoSheet")]
    XlsxNoSheet,
    #[error("XlsxEmptySheet")]
    XlsxEmptySheet,
    #[error("XlsxDataHeader - no column name found at row {} and column {}", .row, .col)]
    XlsxDataHeader { row: String, col: String },
    #[error("XlsxColumnName - name not found for column {}", .col)]
    XlsxColumnName { col: String },
    #[error("XlsxColumnSqlType - sql type not found for column {}", .col)]
    XlsxColumnSqlType { col: String },
    #[error("ConvertFromInt - {0}")]
    ConvertFromInt(#[from] std::num::TryFromIntError),
    #[error("ParseInt - {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("ParseFloat - {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("ConvertInfallible - {0}")]
    ConvertInfallible(#[from] std::convert::Infallible),
    #[error("TokioPostgres - {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),
    #[error("Tiberius - {0}")]
    Tiberius(#[from] tiberius::error::Error),
    #[error("RuSqlite - {0}")]
    RuSqlite(#[from] rusqlite::Error),

    #[error("HasNone")]
    HasNone,
    #[error("DataTypeConversion - conversion error from '{}' to '{}'", .from, .to)]
    DataTypeConversion { from: String, to: String },
    #[error("PgSqlTypeConvesion - conversion error for type: {} (oid: {})", .sql_type, .oid)]
    PgSqlTypeConvesion { sql_type: String, oid: String },
    #[error("{:?} - type definitions do not match value lenght", .source.kind())]
    PgSqlInvalidParameters { source: std::io::Error },
    #[error("DbConnectionClosed")]
    DbConnectionClosed,
    #[error("DeadpoolNoRuntime - {}", .msg)]
    DeadpoolNoRuntime { msg: String },
    #[error("DatabaseConnectionPool - {}", .source)]
    DatabaseConnectionPool {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("SqlBatchLoad - error loading rows between {} - {}. Original error: {}", .start, .end, .source)]
    SqlBatchLoad {
        source: Box<dyn std::error::Error + Send + Sync>,
        start: String,
        end: String,
    },
    #[error("SqlToXlsxValue - conversion error from sql type '{}' for column '{}'", .sql_type, .col_name)]
    SqlToXlsxValue { sql_type: String, col_name: String },
    #[error("SqlNoTableMetadata - no table metadata for '{}.{}'", .schema, .table)]
    SqlNoTableMetadata { schema: String, table: String },
    #[error("MsSqlSessionID")]
    MsSqlSessionID,
}

#[inline]
pub fn error_has_none() -> ErrorReport {
    ErrorReport::HasNone
}

#[inline]
pub fn error_data_type_conversion(from: &(dyn ToString), to: &(dyn ToString)) -> ErrorReport {
    ErrorReport::DataTypeConversion {
        from: from.to_string(),
        to: to.to_string(),
    }
}

#[inline]
pub fn error_pgsql_type_conversion(sql_type: &(dyn ToString), oid: &(dyn ToString)) -> ErrorReport {
    ErrorReport::PgSqlTypeConvesion {
        sql_type: sql_type.to_string(),
        oid: oid.to_string(),
    }
}

#[inline]
pub fn error_xlsx_cell_value(row: &(dyn ToString), col: &(dyn ToString)) -> ErrorReport {
    ErrorReport::XlsxCellValue {
        row: row.to_string(),
        col: col.to_string(),
    }
}

#[inline]
pub fn error_db_connection_closed() -> ErrorReport {
    ErrorReport::DbConnectionClosed
}

#[inline]
pub fn error_deadpool_no_runtime(msg: &(dyn ToString)) -> ErrorReport {
    ErrorReport::DeadpoolNoRuntime {
        msg: msg.to_string(),
    }
}

#[inline]
pub fn error_database_connection_pool(
    source: Box<dyn std::error::Error + Send + Sync>,
) -> ErrorReport {
    ErrorReport::DatabaseConnectionPool { source }
}

#[inline]
pub fn error_timeout() -> ErrorReport {
    std::io::Error::from(std::io::ErrorKind::TimedOut).into()
}

#[inline]
pub fn error_pgsql_invalid_parameters() -> ErrorReport {
    ErrorReport::PgSqlInvalidParameters {
        source: std::io::ErrorKind::InvalidInput.into(),
    }
}

#[inline]
pub fn error_sql_batch_load(
    start: &(dyn ToString),
    end: &(dyn ToString),
    source: Box<dyn std::error::Error + Send + Sync>,
) -> ErrorReport {
    ErrorReport::SqlBatchLoad {
        source,
        start: start.to_string(),
        end: end.to_string(),
    }
}

#[inline]
pub fn error_csv_read_row(
    row: &(dyn ToString),
    source: Box<dyn std::error::Error + Send + Sync>,
) -> ErrorReport {
    ErrorReport::CsvReadRow {
        source,
        row: row.to_string(),
    }
}

#[inline]
pub fn error_xlsx_no_sheet() -> ErrorReport {
    ErrorReport::XlsxNoSheet
}

#[inline]
pub fn error_xlsx_empty_sheet() -> ErrorReport {
    ErrorReport::XlsxEmptySheet
}

#[inline]
pub fn error_xlsx_header(row: &(dyn ToString), col: &(dyn ToString)) -> ErrorReport {
    ErrorReport::XlsxDataHeader {
        row: row.to_string(),
        col: col.to_string(),
    }
}

#[inline]
pub fn error_xlsx_column_name(col: &(dyn ToString)) -> ErrorReport {
    ErrorReport::XlsxColumnName {
        col: col.to_string(),
    }
}

#[inline]
pub fn error_xlsx_column_sql_type(col: &(dyn ToString)) -> ErrorReport {
    ErrorReport::XlsxColumnSqlType {
        col: col.to_string(),
    }
}

#[inline]
pub fn error_sql_to_xlsx_value(
    sql_type: &(dyn ToString),
    col_name: &(dyn ToString),
) -> ErrorReport {
    ErrorReport::SqlToXlsxValue {
        sql_type: sql_type.to_string(),
        col_name: col_name.to_string(),
    }
}

#[inline]
pub fn error_no_table_meta(schema: &(dyn ToString), table: &(dyn ToString)) -> ErrorReport {
    ErrorReport::SqlNoTableMetadata {
        schema: schema.to_string(),
        table: table.to_string(),
    }
}

#[inline]
pub fn error_sql_no_session_id() -> ErrorReport {
    ErrorReport::MsSqlSessionID
}
