use crate::error::ErrorReport;

use super::{FieldMetadata, GenericWrapper};
use tiberius::{ColumnData, IntoSql};

impl tiberius::ToSql for GenericWrapper {
    fn to_sql(&self) -> ColumnData<'_> {
        match self {
            Self::None => Option::<&str>::None.to_sql(),
            Self::Bool(v) => v.to_sql(),
            Self::I8(v) => (*v as i16).into_sql(),
            Self::U8(v) => v.into_sql(),
            Self::I16(v) => v.into_sql(),
            Self::U16(v) => (*v as i32).into_sql(),
            Self::I32(v) => v.into_sql(),
            Self::U32(v) => (*v as i64).into_sql(),
            Self::I64(v) => v.into_sql(),
            Self::U64(v) => {
                let bint = tiberius::numeric::BigInt::from(*v);
                let bdec = tiberius::numeric::BigDecimal::from(bint);
                bdec.into_sql()
            }
            Self::I128(v) => {
                let bint = tiberius::numeric::BigInt::from(*v);
                let bdec = tiberius::numeric::BigDecimal::from(bint);
                bdec.into_sql()
            }
            Self::U128(v) => {
                let bint = tiberius::numeric::BigInt::from(*v);
                let bdec = tiberius::numeric::BigDecimal::from(bint);
                bdec.into_sql()
            }
            Self::F32(v) => v.to_sql(),
            Self::F64(v) => v.to_sql(),
            Self::Char(v) => v.to_string().into_sql(),
            Self::String(v) => v.to_sql(),
            Self::BinVec(v) => v.to_sql(),
            Self::Uuid(v) => v.to_sql(),
            Self::Decimal(v) => tiberius::ToSql::to_sql(v),
            Self::NaiveDate(v) => v.into_sql(),
            Self::NaiveTime(v) => v.into_sql(),
            Self::NaiveTimestamp(v) => v.into_sql(),
            Self::Json(v) => v.to_string().into_sql(),
        }
    }
}

impl tiberius::ToSql for &GenericWrapper {
    fn to_sql(&self) -> ColumnData<'_> {
        (*self).to_sql()
    }
}

impl<'a> tiberius::IntoSql<'a> for GenericWrapper {
    fn into_sql(self) -> ColumnData<'a> {
        match self {
            Self::None => Option::<&str>::None.into_sql(),
            Self::Bool(v) => v.into_sql(),
            Self::I8(v) => (v as i16).into_sql(),
            Self::U8(v) => v.into_sql(),
            Self::I16(v) => v.into_sql(),
            Self::U16(v) => (v as i32).into_sql(),
            Self::I32(v) => v.into_sql(),
            Self::U32(v) => (v as i64).into_sql(),
            Self::I64(v) => v.into_sql(),
            Self::U64(v) => {
                let bint = tiberius::numeric::BigInt::from(v);
                let bdec = tiberius::numeric::BigDecimal::from(bint);
                bdec.into_sql()
            }
            Self::I128(v) => {
                let bint = tiberius::numeric::BigInt::from(v);
                let bdec = tiberius::numeric::BigDecimal::from(bint);
                bdec.into_sql()
            }
            Self::U128(v) => {
                let bint = tiberius::numeric::BigInt::from(v);
                let bdec = tiberius::numeric::BigDecimal::from(bint);
                bdec.into_sql()
            }
            Self::F32(v) => v.into_sql(),
            Self::F64(v) => v.into_sql(),
            Self::Char(v) => v.to_string().into_sql(),
            Self::String(v) => v.into_sql(),
            Self::BinVec(v) => v.into_sql(),
            Self::Uuid(v) => v.into_sql(),
            Self::Decimal(v) => {
                let unpacked = v.unpack();
                let mut value = (((unpacked.hi as u128) << 64)
                    + ((unpacked.mid as u128) << 32)
                    + unpacked.lo as u128) as i128;
                if v.is_sign_negative() {
                    value = -value;
                }
                tiberius::numeric::Numeric::new_with_scale(value, v.scale() as u8).into_sql()
            }
            Self::NaiveDate(v) => v.into_sql(),
            Self::NaiveTime(v) => v.into_sql(),
            Self::NaiveTimestamp(v) => v.into_sql(),
            Self::Json(v) => v.to_string().into_sql(),
        }
    }
}

impl<'a> tiberius::FromSql<'a> for GenericWrapper {
    fn from_sql(value: &'a ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        let res = match value {
            ColumnData::U8(v) => Self::from(v),
            ColumnData::I16(v) => Self::from(v),
            ColumnData::I32(v) => Self::from(v),
            ColumnData::I64(v) => Self::from(v),
            ColumnData::F32(v) => Self::from(v),
            ColumnData::F64(v) => Self::from(v),
            ColumnData::Bit(v) => Self::from(v),
            ColumnData::String(v) => Self::from(v.as_ref().map(|v| v.as_ref())),
            ColumnData::Guid(v) => Self::from(v),
            ColumnData::Binary(v) => Self::from(v.as_ref().map(|v| v.as_ref())),
            ColumnData::Numeric(_) => Self::from(rust_decimal::Decimal::from_sql(value)?),
            ColumnData::Xml(v) => Self::from(v.to_owned().map(|v| v.to_string())),
            ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) | ColumnData::DateTime2(_) => {
                Self::from(chrono::NaiveDateTime::from_sql(value)?)
            }
            ColumnData::Time(_) => Self::from(chrono::NaiveTime::from_sql(value)?),
            ColumnData::Date(_) => Self::from(chrono::NaiveDate::from_sql(value)?),
            ColumnData::DateTimeOffset(_) => {
                return Err(tiberius::error::Error::Conversion(std::borrow::Cow::from(
                    "There is no conversion implemented for mssql type 'datetimeoffset'",
                )))
            }
        };
        Ok(Some(res))
    }
}

impl tiberius::FromSqlOwned for GenericWrapper {
    fn from_sql_owned(value: ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        <Self as tiberius::FromSql>::from_sql(&value)
    }
}

const SQL_TYPES_WITH_SCALE: [&str; 2] = ["numeric", "decimal"];

impl super::FieldMetadata {
    pub fn mssql_column_name(&self) -> String {
        format!("[{}]", self.name)
    }

    pub fn mssql_type_name(&self) -> String {
        if let Some(val) = self.character_maximum_length {
            return format!("{}({})", self.udt_name, val);
        }

        if SQL_TYPES_WITH_SCALE.iter().any(|v| v.eq(&self.udt_name))
            && self.numeric_precision.is_some()
            && self.numeric_scale.is_some()
        {
            format!(
                "{}({}, {})",
                self.udt_name,
                self.numeric_precision.unwrap(),
                self.numeric_scale.unwrap()
            )
        } else {
            self.udt_name.to_owned()
        }
    }
}

impl TryFrom<tiberius::Row> for FieldMetadata {
    type Error = ErrorReport;

    fn try_from(row: tiberius::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            name: {
                let v: Option<&str> = row.try_get("column_name")?;
                match v {
                    Some(r) => r.to_owned(),
                    None => return Err(crate::error::error_has_none()),
                }
            },
            character_maximum_length: row.try_get("character_maximum_length")?,
            numeric_precision: row.try_get::<u8, _>("numeric_precision")?.map(|v| v as i32),
            numeric_precision_radix: row
                .try_get::<i16, _>("numeric_precision_radix")?
                .map(|v| v as i32),
            numeric_scale: row.try_get("numeric_scale")?,
            udt_name: {
                let v: Option<&str> = row.try_get("data_type")?;
                match v {
                    Some(r) => r.to_owned(),
                    None => return Err(crate::error::error_has_none()),
                }
            },
            _oid: 0,
        })
    }
}

impl TryFrom<tiberius::Row> for super::GenericSqlRow<String, GenericWrapper> {
    type Error = ErrorReport;

    fn try_from(row: tiberius::Row) -> Result<Self, Self::Error> {
        let mut res = indexmap::IndexMap::new();
        for i in 0..row.len() {
            res.insert(
                row.columns()[i].name().into(),
                row.try_get::<GenericWrapper, _>(i)?.into(),
            );
        }
        Ok(Self(res))
    }
}
