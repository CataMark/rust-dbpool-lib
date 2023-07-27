use rusqlite::types::FromSql;

use crate::error::ErrorReport;

impl From<rusqlite::types::ValueRef<'_>> for super::GenericWrapper {
    fn from(v: rusqlite::types::ValueRef<'_>) -> Self {
        match v {
            rusqlite::types::ValueRef::Null => Self::None,
            rusqlite::types::ValueRef::Integer(p) => Self::from(p),
            rusqlite::types::ValueRef::Real(p) => Self::from(p),
            rusqlite::types::ValueRef::Text(p) => {
                if let Ok(r) = chrono::NaiveDateTime::column_result(v) {
                    Self::NaiveTimestamp(r)
                } else if let Ok(r) = chrono::NaiveTime::column_result(v) {
                    Self::NaiveTime(r)
                } else if let Ok(r) = chrono::NaiveDate::column_result(v) {
                    Self::NaiveDate(r)
                } else if let Ok(r) = uuid::Uuid::column_result(v) {
                    Self::Uuid(r)
                } else {
                    Self::from(String::from_utf8_lossy(p))
                }
            }
            rusqlite::types::ValueRef::Blob(p) => {
                if let Ok(r) = uuid::Uuid::column_result(v) {
                    Self::Uuid(r)
                } else if let Ok(r) = i128::column_result(v) {
                    Self::I128(r)
                } else {
                    Self::from(p)
                }
            }
        }
    }
}

impl From<rusqlite::types::Value> for super::GenericWrapper {
    fn from(v: rusqlite::types::Value) -> Self {
        Self::from(rusqlite::types::ValueRef::from(&v))
    }
}

impl rusqlite::types::FromSql for super::GenericWrapper {
    fn column_result(v: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(Self::from(v))
    }
}

impl rusqlite::ToSql for super::GenericWrapper {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let error = self.into_error("rusqlite::ToSql");
        match self {
            Self::None => rusqlite::types::Null.to_sql(),
            Self::Bool(v) => v.to_sql(),
            Self::I8(v) => v.to_sql(),
            Self::U8(v) => v.to_sql(),
            Self::I16(v) => v.to_sql(),
            Self::U16(v) => v.to_sql(),
            Self::I32(v) => v.to_sql(),
            Self::U32(v) => v.to_sql(),
            Self::I64(v) => v.to_sql(),
            Self::U64(v) => v.to_sql(),
            Self::I128(v) => v.to_sql(),
            Self::F32(v) => v.to_sql(),
            Self::F64(v) => v.to_sql(),
            Self::String(v) => v.to_sql(),
            Self::BinVec(v) => v.to_sql(),
            Self::Uuid(v) => v.to_sql(),
            Self::NaiveDate(v) => v.to_sql(),
            Self::NaiveTime(v) => v.to_sql(),
            Self::NaiveTimestamp(v) => v.to_sql(),
            Self::Json(v) => v.to_sql(),
            _ => Err(rusqlite::Error::ToSqlConversionFailure(error.into())),
        }
    }
}

impl super::FieldMetadata {
    pub fn sqlite_column_name(&self) -> String {
        format!(r#""{}""#, self.name)
    }

    pub fn sqlite_type_name(&self) -> String {
        self.udt_name.to_owned()
    }
}

impl TryFrom<&rusqlite::Row<'_>> for super::FieldMetadata {
    type Error = ErrorReport;

    fn try_from(row: &rusqlite::Row<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: row.get("name")?,
            character_maximum_length: None,
            numeric_precision: None,
            numeric_precision_radix: None,
            numeric_scale: None,
            udt_name: row.get("type")?,
            _oid: 0,
        })
    }
}

impl TryFrom<&rusqlite::Row<'_>> for super::GenericSqlRow<String, super::GenericWrapper> {
    type Error = ErrorReport;

    fn try_from(row: &rusqlite::Row<'_>) -> Result<Self, Self::Error> {
        let stmt = row.as_ref();
        let mut res = indexmap::IndexMap::new();
        for name in stmt.column_names() {
            res.insert(name.into(), row.get_ref(name)?.into());
        }
        Ok(Self(res))
    }
}
