#[cfg(feature = "mssql")]
pub mod mssql;
#[cfg(feature = "pgsql")]
pub mod pgsql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use crate::error::ErrorReport;
use regex::Regex;
use serde::{
    de::{Deserialize, Deserializer, MapAccess},
    Serialize,
};
use std::{fmt::Debug, iter::FromIterator};

const CHRONO_DATE_FORMAT: &str = "%Y-%m-%d";
const CHRONO_TIME_FORMAT: &str = "%H:%M:%S";
const CHRONO_TMSTAMP_ANSI_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
const CHRONO_TMSTAMP_ISO_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";

lazy_static::lazy_static! {
    static ref UUID_REGEX_PATT: Result<Regex, regex::Error> = Regex::new(r"^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$");
    static ref DATE_REGEX_PATT: Result<Regex, regex::Error> = Regex::new(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$$"); // yyyy-mm-dd
    static ref TIME_REGEX_PATT: Result<Regex, regex::Error> = Regex::new(r"^(?:(?:([01]?\d|2[0-3]):)?([0-5]?\d):)?([0-5]?\d)$"); // HH:MM:ss
    static ref TMSTAMP_ANSI_REGEX_PATT: Result<Regex, regex::Error> = Regex::new(r"^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]) (?:(?:([01]?\d|2[0-3]):)?([0-5]?\d):)?([0-5]?\d)$"); // yyyy-mm-dd HH:MM:ss
    static ref TMSTAMP_ISO_REGEX_PATT: Result<Regex, regex::Error> = Regex::new(r"^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])T(?:(?:([01]?\d|2[0-3]):)?([0-5]?\d):)?([0-5]?\d)$"); // yyyy-mm-ddTHH:MM:ss
    static ref JSON_REGEX_PATT: Result<Regex, regex::Error> = Regex::new(r"^(\{|\[)(.|\r|\n)*(\]|\})$");
}

#[derive(Debug, Clone, PartialEq)]
pub enum GenericWrapper {
    None,
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    F32(f32),
    F64(f64),
    Char(char),
    String(String),
    BinVec(Vec<u8>),
    Uuid(uuid::Uuid),
    Decimal(rust_decimal::Decimal),
    NaiveDate(chrono::NaiveDate),
    NaiveTime(chrono::NaiveTime),
    NaiveTimestamp(chrono::NaiveDateTime),
    Json(serde_json::Value),
}

impl GenericWrapper {
    pub fn type_hint(&self) -> &str {
        match self {
            Self::Bool(_) => "bool",
            Self::I8(_) => "i8",
            Self::U8(_) => "u8",
            Self::I16(_) => "i16",
            Self::U16(_) => "u16",
            Self::I32(_) => "i32",
            Self::U32(_) => "u32",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
            Self::I128(_) => "i128",
            Self::U128(_) => "u128",
            Self::F32(_) => "f32",
            Self::F64(_) => "f64",
            Self::Char(_) => "char",
            Self::String(_) => "String",
            Self::BinVec(_) => "Vec<u8>",
            Self::Uuid(_) => "uuid::Uuid",
            Self::Decimal(_) => "rust_decimal::Decimal",
            Self::NaiveDate(_) => "chrono::NaiveDate",
            Self::NaiveTime(_) => "chrono::NaiveTime",
            Self::NaiveTimestamp(_) => "chrono::NaiveDateTime",
            Self::Json(_) => "serde_json::Value",
            Self::None => "none",
        }
    }

    pub fn into_error(&self, target_type: &str) -> ErrorReport {
        crate::error::error_data_type_conversion(&self.type_hint(), &target_type)
    }

    pub fn has_none(&self) -> bool {
        matches!(self, Self::None | Self::Json(serde_json::Value::Null))
    }

    fn parse_string(input: String) -> Self {
        let v = input.trim();

        if v.is_empty() {
            return Self::None;
        }

        if let Ok(regex) = UUID_REGEX_PATT.as_ref() {
            if regex.is_match(v) {
                if let Ok(res) = uuid::Uuid::try_from(v) {
                    return Self::Uuid(res);
                }
            }
        }

        if let Ok(regex) = DATE_REGEX_PATT.as_ref() {
            if regex.is_match(v) {
                if let Ok(res) = chrono::NaiveDate::parse_from_str(v, CHRONO_DATE_FORMAT) {
                    return Self::NaiveDate(res);
                }
            }
        }

        if let Ok(regex) = TIME_REGEX_PATT.as_ref() {
            if regex.is_match(v) {
                if let Ok(res) = chrono::NaiveTime::parse_from_str(v, CHRONO_TIME_FORMAT) {
                    return Self::NaiveTime(res);
                }
            }
        }

        if let Ok(regex) = TMSTAMP_ANSI_REGEX_PATT.as_ref() {
            if regex.is_match(v) {
                if let Ok(res) =
                    chrono::NaiveDateTime::parse_from_str(v, CHRONO_TMSTAMP_ANSI_FORMAT)
                {
                    return Self::NaiveTimestamp(res);
                }
            }
        }

        if let Ok(regex) = TMSTAMP_ISO_REGEX_PATT.as_ref() {
            if regex.is_match(v) {
                if let Ok(res) = chrono::NaiveDateTime::parse_from_str(v, CHRONO_TMSTAMP_ISO_FORMAT)
                {
                    return Self::NaiveTimestamp(res);
                }
            }
        }

        if let Ok(regex) = JSON_REGEX_PATT.as_ref() {
            if regex.is_match(v) {
                if let Ok(res) = serde_json::from_str(v) {
                    return Self::Json(res);
                }
            }
        }

        Self::String(input)
    }

    fn parse_json(input: serde_json::Value) -> Self {
        match &input {
            serde_json::Value::Null => Self::None,
            serde_json::Value::Bool(v) => Self::Bool(v.to_owned()),
            serde_json::Value::String(v) => Self::parse_string(v.to_owned()),
            serde_json::Value::Number(v) => {
                if let Some(n) = v.as_i64() {
                    Self::I64(n)
                } else if let Some(n) = v.as_u64() {
                    Self::U64(n)
                } else if let Some(n) = v.as_f64() {
                    Self::F64(n)
                } else {
                    Self::Json(input)
                }
            }
            _ => Self::Json(input),
        }
    }
}

impl Default for GenericWrapper {
    fn default() -> Self {
        Self::None
    }
}

impl AsRef<GenericWrapper> for GenericWrapper {
    fn as_ref(&self) -> &GenericWrapper {
        self
    }
}

macro_rules! simple_from {
    ($ty:ty, $arm:ident, owned) => {
        impl From<$ty> for GenericWrapper {
            fn from(v: $ty) -> Self {
                Self::$arm(v)
            }
        }
    };

    ($ty:ty, $arm:ident, deriv) => {
        impl<'a> From<&'a $ty> for GenericWrapper {
            fn from(v: &'a $ty) -> Self {
                Self::from(v.to_owned())
            }
        }

        impl From<Option<$ty>> for GenericWrapper {
            fn from(v: Option<$ty>) -> Self {
                match v {
                    Some(r) => Self::from(r),
                    None => Self::None,
                }
            }
        }

        impl<'a> From<Option<&'a $ty>> for GenericWrapper {
            fn from(v: Option<&'a $ty>) -> Self {
                match v {
                    Some(r) => Self::from(r),
                    None => Self::None,
                }
            }
        }

        impl<'a> From<&'a Option<$ty>> for GenericWrapper {
            fn from(v: &'a Option<$ty>) -> Self {
                match v {
                    Some(r) => Self::from(r),
                    None => Self::None,
                }
            }
        }
    };

    ($ty:ty, $arm:ident, all) => {
        simple_from!($ty, $arm, owned);
        simple_from!($ty, $arm, deriv);
    };
}

macro_rules! _simple_tryinto {
    ($ty:ty, $arm:ident) => {
        impl TryInto<Option<$ty>> for GenericWrapper {
            type Error = ErrorReport;

            fn try_into(self) -> Result<Option<$ty>, Self::Error> {
                if self.has_none() {
                    return Ok(None);
                }

                let res = match self {
                    Self::$arm(v) => v,
                    _ => return Err(self.into_error(stringify!($ty))),
                };
                Ok(Some(res))
            }
        }

        impl TryInto<$ty> for GenericWrapper {
            type Error = ErrorReport;

            fn try_into(self) -> Result<$ty, Self::Error> {
                let res: Option<$ty> = self.try_into()?;
                match res {
                    Some(v) => Ok(v),
                    None => Err(crate::error::error_has_none()),
                }
            }
        }
    };
}

macro_rules! int_tryinto {
    ($ty:ty) => {
        impl TryInto<Option<$ty>> for GenericWrapper {
            type Error = ErrorReport;

            fn try_into(self) -> Result<Option<$ty>, Self::Error> {
                if self.has_none() {
                    return Ok(None);
                }
                let error = self.into_error(stringify!($ty));

                let res: $ty = match self {
                    Self::I8(v) => v.try_into()?,
                    Self::U8(v) => v.try_into()?,
                    Self::I16(v) => v.try_into()?,
                    Self::U16(v) => v.try_into()?,
                    Self::I32(v) => v.try_into()?,
                    Self::U32(v) => v.try_into()?,
                    Self::I64(v) => v.try_into()?,
                    Self::U64(v) => v.try_into()?,
                    Self::I128(v) => v.try_into()?,
                    Self::U128(v) => v.try_into()?,
                    Self::String(v) => v.parse::<$ty>()?,
                    Self::Decimal(v) => v.try_into()?,
                    Self::Json(serde_json::Value::Number(v)) => {
                        if let Some(r) = v.as_i64() {
                            r.try_into()?
                        } else if let Some(r) = v.as_u64() {
                            r.try_into()?
                        } else {
                            return Err(error);
                        }
                    }
                    Self::Json(serde_json::Value::String(v)) => v.parse::<$ty>()?,
                    _ => return Err(error),
                };
                Ok(Some(res))
            }
        }

        impl TryInto<$ty> for GenericWrapper {
            type Error = ErrorReport;

            fn try_into(self) -> Result<$ty, Self::Error> {
                let res: Option<$ty> = self.try_into()?;
                match res {
                    Some(v) => Ok(v),
                    None => Err(crate::error::error_has_none()),
                }
            }
        }
    };
}

macro_rules! tryinto_from_option_impl {
    ($ty:ty) => {
        impl TryInto<$ty> for GenericWrapper {
            type Error = ErrorReport;

            fn try_into(self) -> Result<$ty, Self::Error> {
                let res: Option<$ty> = self.try_into()?;
                match res {
                    Some(v) => Ok(v),
                    None => Err(crate::error::error_has_none()),
                }
            }
        }
    };
}

simple_from!(bool, Bool, all);
simple_from!(i8, I8, all);
simple_from!(u8, U8, all);
simple_from!(i16, I16, all);
simple_from!(u16, U16, all);
simple_from!(i32, I32, all);
simple_from!(u32, U32, all);
simple_from!(i64, I64, all);
simple_from!(u64, U64, all);
simple_from!(i128, I128, all);
simple_from!(u128, U128, all);
simple_from!(f32, F32, all);
simple_from!(f64, F64, all);
simple_from!(char, Char, all);
simple_from!(String, String, all);
simple_from!(Vec<u8>, BinVec, all);
simple_from!(uuid::Uuid, Uuid, all);
simple_from!(rust_decimal::Decimal, Decimal, all);
simple_from!(chrono::NaiveDate, NaiveDate, all);
simple_from!(chrono::NaiveTime, NaiveTime, all);
simple_from!(chrono::NaiveDateTime, NaiveTimestamp, all);
simple_from!(serde_json::Value, Json, all);

impl<'a> From<&'a str> for GenericWrapper {
    fn from(v: &'a str) -> Self {
        Self::from(v.to_string())
    }
}

impl<'a> From<Option<&'a str>> for GenericWrapper {
    fn from(v: Option<&'a str>) -> Self {
        match v {
            Some(v) => Self::from(v.to_string()),
            None => Self::None,
        }
    }
}

impl<'a> From<std::borrow::Cow<'a, str>> for GenericWrapper {
    fn from(v: std::borrow::Cow<'a, str>) -> Self {
        Self::from(&*v)
    }
}

impl<'a> From<Option<std::borrow::Cow<'a, str>>> for GenericWrapper {
    fn from(v: Option<std::borrow::Cow<'a, str>>) -> Self {
        match v {
            Some(v) => Self::from(v),
            None => Self::None,
        }
    }
}

impl<'a> From<&'a [u8]> for GenericWrapper {
    fn from(v: &'a [u8]) -> Self {
        Self::BinVec(v.to_vec())
    }
}

impl<'a> From<Option<&'a [u8]>> for GenericWrapper {
    fn from(v: Option<&'a [u8]>) -> Self {
        match v {
            Some(v) => Self::from(v),
            None => Self::None,
        }
    }
}

simple_from!(GenericWrapper, None, deriv);

int_tryinto!(i8);
int_tryinto!(u8);
int_tryinto!(i16);
int_tryinto!(u16);
int_tryinto!(i32);
int_tryinto!(u32);
int_tryinto!(i64);
int_tryinto!(u64);
int_tryinto!(i128);
int_tryinto!(u128);

impl TryInto<Option<bool>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<bool>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let res = match self {
            Self::Bool(v) => v,
            Self::Json(serde_json::Value::Bool(v)) => v,
            _ => return Err(self.into_error(stringify!(f32))),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(bool);

impl TryInto<Option<String>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<String>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let res = match self {
            Self::String(v) => v,
            Self::Json(serde_json::Value::String(v)) => v,
            _ => return Err(self.into_error(stringify!(f32))),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(String);

impl TryInto<Option<Vec<u8>>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<Vec<u8>>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let error = self.into_error(stringify!(f32));
        let res = match self {
            Self::BinVec(v) => v,
            Self::Json(serde_json::Value::Array(v)) => {
                let mut r: Vec<u8> = Vec::new();
                for p in v {
                    if let Some(p) = p.as_u64() {
                        r.push(p.try_into()?)
                    } else {
                        return Err(error);
                    }
                }
                r
            }
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(Vec<u8>);

impl TryInto<Option<f32>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<f32>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }

        let error = self.into_error(stringify!(f32));
        let res: f32 = match self {
            Self::I8(v) => f32::from(v),
            Self::U8(v) => f32::from(v),
            Self::I16(v) => f32::from(v),
            Self::U16(v) => f32::from(v),
            Self::F32(v) => v,
            Self::String(v) => v.parse::<f32>()?,
            Self::Decimal(v) => v.try_into()?,
            Self::Json(serde_json::Value::Number(v)) => {
                if let Some(p) = v.as_f64().map(|i| i as f32) {
                    if p.is_finite() {
                        p
                    } else {
                        return Err(error);
                    }
                } else {
                    return Err(error);
                }
            }
            Self::Json(serde_json::Value::String(v)) => v.parse::<f32>()?,
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(f32);

impl TryInto<Option<f64>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<f64>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let error = self.into_error(stringify!(f64));
        let res = match self {
            Self::I8(v) => f64::from(v),
            Self::U8(v) => f64::from(v),
            Self::I16(v) => f64::from(v),
            Self::U16(v) => f64::from(v),
            Self::I32(v) => f64::from(v),
            Self::U32(v) => f64::from(v),
            Self::F32(v) => f64::from(v),
            Self::F64(v) => v,
            Self::String(v) => v.parse::<f64>()?,
            Self::Decimal(v) => v.try_into()?,
            Self::Json(serde_json::Value::Number(v)) => {
                if let Some(p) = v.as_f64() {
                    p
                } else {
                    return Err(error);
                }
            }
            Self::Json(serde_json::Value::String(v)) => v.parse::<f64>()?,
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(f64);

impl TryInto<Option<rust_decimal::Decimal>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<rust_decimal::Decimal>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }

        let error = self.into_error(stringify!(rust_decimal::Decimal));
        let res = match self {
            Self::I8(v) => rust_decimal::Decimal::from(v),
            Self::U8(v) => rust_decimal::Decimal::from(v),
            Self::I16(v) => rust_decimal::Decimal::from(v),
            Self::U16(v) => rust_decimal::Decimal::from(v),
            Self::I32(v) => rust_decimal::Decimal::from(v),
            Self::U32(v) => rust_decimal::Decimal::from(v),
            Self::I64(v) => rust_decimal::Decimal::from(v),
            Self::U64(v) => rust_decimal::Decimal::from(v),
            Self::I128(v) => rust_decimal::Decimal::from(v),
            Self::U128(v) => rust_decimal::Decimal::from(v),
            Self::F32(v) => rust_decimal::Decimal::try_from(v)?,
            Self::F64(v) => rust_decimal::Decimal::try_from(v)?,
            Self::String(v) => rust_decimal::Decimal::from_str_exact(v.as_str())?,
            Self::Decimal(v) => v,
            Self::Json(serde_json::Value::Number(v)) => {
                if let Some(p) = v.as_i64() {
                    rust_decimal::Decimal::try_from(p)?
                } else if let Some(p) = v.as_u64() {
                    rust_decimal::Decimal::try_from(p)?
                } else if let Some(p) = v.as_f64() {
                    rust_decimal::Decimal::try_from(p)?
                } else {
                    return Err(error);
                }
            }
            Self::Json(serde_json::Value::String(v)) => {
                rust_decimal::Decimal::from_str_exact(v.as_str())?
            }
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(rust_decimal::Decimal);

impl TryInto<Option<char>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<char>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let res = match self {
            Self::Char(v) => v,
            Self::String(v) if v.is_empty() => char::from(v.as_bytes()[0]),
            Self::Json(serde_json::Value::String(v)) if v.is_empty() => char::from(v.as_bytes()[0]),
            _ => return Err(self.into_error(stringify!(char))),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(char);

impl TryInto<Option<uuid::Uuid>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<uuid::Uuid>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let regex = UUID_REGEX_PATT.as_ref().map_err(ToOwned::to_owned)?;
        let error = self.into_error(stringify!(uuid::Uuid));
        let by_regex = |v: String, e: Self::Error| {
            let p = v.trim();
            if regex.is_match(p) {
                Ok(uuid::Uuid::try_from(p)?)
            } else {
                Err(e)
            }
        };
        let res = match self {
            Self::Uuid(v) => v,
            Self::String(v) => by_regex(v, error)?,
            Self::Json(serde_json::Value::String(v)) => by_regex(v, error)?,
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(uuid::Uuid);

impl TryInto<Option<chrono::NaiveDate>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<chrono::NaiveDate>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let regex = DATE_REGEX_PATT.as_ref().map_err(ToOwned::to_owned)?;
        let error = self.into_error(stringify!(chrono::NaiveDate));
        let by_regex = |v: String, e: Self::Error| {
            let p = v.trim();
            if regex.is_match(p) {
                Ok(chrono::NaiveDate::parse_from_str(p, CHRONO_DATE_FORMAT)?)
            } else {
                Err(e)
            }
        };
        let res = match self {
            Self::NaiveDate(v) => v,
            Self::String(v) => by_regex(v, error)?,
            Self::Json(serde_json::Value::String(v)) => by_regex(v, error)?,
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(chrono::NaiveDate);

impl TryInto<Option<chrono::NaiveTime>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<chrono::NaiveTime>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let regex = TIME_REGEX_PATT.as_ref().map_err(ToOwned::to_owned)?;
        let error = self.into_error(stringify!(chrono::NaiveTime));
        let by_regex = |v: String, e: Self::Error| {
            let p = v.trim();
            if regex.is_match(p) {
                Ok(chrono::NaiveTime::parse_from_str(p, CHRONO_TIME_FORMAT)?)
            } else {
                Err(e)
            }
        };
        let res = match self {
            Self::NaiveTime(v) => v,
            Self::String(v) => by_regex(v, error)?,
            Self::Json(serde_json::Value::String(v)) => by_regex(v, error)?,
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(chrono::NaiveTime);

impl TryInto<Option<chrono::NaiveDateTime>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<chrono::NaiveDateTime>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let regex1 = TMSTAMP_ANSI_REGEX_PATT
            .as_ref()
            .map_err(ToOwned::to_owned)?;
        let regex2 = TMSTAMP_ISO_REGEX_PATT.as_ref().map_err(ToOwned::to_owned)?;
        let error = self.into_error(stringify!(chrono::NaiveDateTime));
        let by_regex = |v: String, e: Self::Error| {
            let p = v.trim();
            if regex1.is_match(p) {
                Ok(chrono::NaiveDateTime::parse_from_str(
                    p,
                    CHRONO_TMSTAMP_ANSI_FORMAT,
                )?)
            } else if regex2.is_match(p) {
                Ok(chrono::NaiveDateTime::parse_from_str(
                    p,
                    CHRONO_TMSTAMP_ISO_FORMAT,
                )?)
            } else {
                Err(e)
            }
        };

        let res = match self {
            Self::NaiveTimestamp(v) => v,
            Self::String(v) => by_regex(v, error)?,
            Self::Json(serde_json::Value::String(v)) => by_regex(v, error)?,
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(chrono::NaiveDateTime);

impl TryInto<Option<serde_json::Value>> for GenericWrapper {
    type Error = ErrorReport;

    fn try_into(self) -> Result<Option<serde_json::Value>, Self::Error> {
        if self.has_none() {
            return Ok(None);
        }
        let regex = JSON_REGEX_PATT.as_ref().map_err(ToOwned::to_owned)?;
        let error = self.into_error(stringify!(serde_json::Value));
        let res = match self {
            Self::Json(v) => v,
            Self::String(v) => {
                let p = v.trim();
                if regex.is_match(p) {
                    serde_json::from_str(p)?
                } else {
                    return Err(error);
                }
            }
            _ => return Err(error),
        };
        Ok(Some(res))
    }
}

tryinto_from_option_impl!(serde_json::Value);

impl Serialize for GenericWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            GenericWrapper::None => serializer.serialize_none(),
            GenericWrapper::Bool(v) => serializer.serialize_bool(v.to_owned()),
            GenericWrapper::I8(v) => serializer.serialize_i8(v.to_owned()),
            GenericWrapper::U8(v) => serializer.serialize_u8(v.to_owned()),
            GenericWrapper::I16(v) => serializer.serialize_i16(v.to_owned()),
            GenericWrapper::U16(v) => serializer.serialize_u16(v.to_owned()),
            GenericWrapper::I32(v) => serializer.serialize_i32(v.to_owned()),
            GenericWrapper::U32(v) => serializer.serialize_u32(v.to_owned()),
            GenericWrapper::I64(v) => serializer.serialize_i64(v.to_owned()),
            GenericWrapper::U64(v) => serializer.serialize_u64(v.to_owned()),
            GenericWrapper::I128(v) => serializer.serialize_i128(v.to_owned()),
            GenericWrapper::U128(v) => serializer.serialize_u128(v.to_owned()),
            GenericWrapper::F32(v) => serializer.serialize_f32(v.to_owned()),
            GenericWrapper::F64(v) => serializer.serialize_f64(v.to_owned()),
            GenericWrapper::Char(v) => serializer.serialize_char(v.to_owned()),
            GenericWrapper::String(v) => serializer.serialize_str(v.as_str()),
            GenericWrapper::BinVec(v) => v.serialize(serializer),
            GenericWrapper::Uuid(v) => v.serialize(serializer),
            GenericWrapper::Decimal(v) => serializer.serialize_str(v.to_string().as_str()),
            GenericWrapper::NaiveDate(v) => {
                serializer.serialize_str(v.format(CHRONO_DATE_FORMAT).to_string().as_str())
            }
            GenericWrapper::NaiveTime(v) => {
                serializer.serialize_str(v.format(CHRONO_TIME_FORMAT).to_string().as_str())
            }
            GenericWrapper::NaiveTimestamp(v) => {
                serializer.serialize_str(v.format(CHRONO_TMSTAMP_ANSI_FORMAT).to_string().as_str())
            }
            GenericWrapper::Json(v) => v.serialize(serializer),
        }
    }
}

struct GenericWrapperVisitor;

macro_rules! visitor_method {
    ($ty:ty, $arm:ident) => {
        paste::paste! {
            fn [<visit_$ty>]<E>(self, v: $ty) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Self::Value::$arm(v))
            }
        }
    };
}

impl<'de> serde::de::Visitor<'de> for GenericWrapperVisitor {
    type Value = GenericWrapper;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "an enum")
    }

    visitor_method!(bool, Bool);
    visitor_method!(i8, I8);
    visitor_method!(i16, I16);
    visitor_method!(i32, I32);
    visitor_method!(i64, I64);
    visitor_method!(i128, I128);
    visitor_method!(u8, U8);
    visitor_method!(u16, U16);
    visitor_method!(u32, U32);
    visitor_method!(u64, U64);
    visitor_method!(u128, U128);
    visitor_method!(f32, F32);
    visitor_method!(f64, F64);
    visitor_method!(char, Char);

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Self::Value::parse_string(v.to_string()))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Self::Value::parse_string(v.to_string()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Self::Value::parse_string(v))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Self::Value::from(v))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Self::Value::from(v))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Self::Value::from(v))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Self::Value::None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let res = if let Some(v) = Option::<serde_json::Value>::deserialize(deserializer)? {
            Self::Value::parse_json(v)
        } else {
            Self::Value::None
        };
        Ok(res)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Self::Value::None)
    }

    /*fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!()
    }*/

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut seq = seq;
        let mut vec = Vec::new();
        while let Some(v) = seq.next_element()? {
            vec.push(v);
        }
        Ok(Self::Value::Json(serde_json::Value::Array(vec)))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut map = map;
        let mut res = serde_json::Map::new();
        while let Some((k, v)) = map.next_entry()? {
            res.insert(k, v);
        }
        Ok(Self::Value::Json(serde_json::Value::Object(res)))
    }

    /*fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        todo!()
    }*/
}

impl<'de> Deserialize<'de> for GenericWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(GenericWrapperVisitor)
    }
}

#[derive(Debug, Clone)]
pub struct FieldMetadata {
    name: String,
    character_maximum_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_precision_radix: Option<i32>,
    numeric_scale: Option<i32>,
    udt_name: String,
    _oid: u32,
}

impl FieldMetadata {
    pub fn name(&self) -> String {
        self.name.to_owned()
    }
}

impl PartialEq for FieldMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.name.to_lowercase() == other.name.to_lowercase()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDefault<'a, T> {
    Formula(&'a str),
    Value(T),
    ByDatabase,
}

#[derive(Debug, Clone)]
pub struct GenericSqlRow<K, V>(indexmap::IndexMap<K, V>);

impl<K, V> GenericSqlRow<K, V> {
    pub fn new() -> Self {
        let map = indexmap::IndexMap::<K, V>::new();
        Self(map)
    }
}

impl<K, V> Default for GenericSqlRow<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> From<GenericSqlRow<K, V>> for indexmap::IndexMap<K, V> {
    fn from(v: GenericSqlRow<K, V>) -> Self {
        v.0
    }
}

impl<K, V> From<indexmap::IndexMap<K, V>> for GenericSqlRow<K, V> {
    fn from(v: indexmap::IndexMap<K, V>) -> Self {
        Self(v)
    }
}

impl<'a, K, V> From<&'a indexmap::IndexMap<K, V>> for GenericSqlRow<K, V>
where
    K: Clone,
    V: Clone,
{
    fn from(v: &'a indexmap::IndexMap<K, V>) -> Self {
        v.clone().into()
    }
}

impl<K, V> AsRef<indexmap::IndexMap<K, V>> for GenericSqlRow<K, V> {
    fn as_ref(&self) -> &indexmap::IndexMap<K, V> {
        &self.0
    }
}

impl<K, V> Serialize for GenericSqlRow<K, V>
where
    K: std::hash::Hash + Eq + Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let map: &indexmap::IndexMap<K, V> = self.as_ref();
        map.serialize(serializer)
    }
}

impl<'de, K, V> Deserialize<'de> for GenericSqlRow<K, V>
where
    K: std::hash::Hash + Eq + Deserialize<'de>,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map = indexmap::IndexMap::<K, V>::deserialize(deserializer)?;
        Ok(Self(map))
    }
}

impl<K, V> FromIterator<(K, V)> for GenericSqlRow<K, V>
where
    K: std::hash::Hash + Eq,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let map = indexmap::IndexMap::<K, V>::from_iter(iter);
        Self(map)
    }
}

impl<K, V> IntoIterator for GenericSqlRow<K, V> {
    type Item = (K, V);
    type IntoIter = indexmap::map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::generics::{GenericSqlRow, GenericWrapper};
    use indexmap::IndexMap;

    #[test]
    fn regex() {
        let uuid = uuid::Uuid::new_v4().to_string();
        let generic: uuid::Uuid = GenericWrapper::from(&uuid).try_into().unwrap();
        assert_eq!(uuid, generic.to_string(), "UUID strings don't match");

        let date = "2022-01-03";
        let generic: chrono::NaiveDate = GenericWrapper::from(date).try_into().unwrap();
        assert_eq!(date, generic.to_string(), "Date strings don't match");

        let time = "18:30:15";
        let generic: chrono::NaiveTime = GenericWrapper::from(time).try_into().unwrap();
        assert_eq!(time, generic.to_string(), "Time strings don't match");

        let timestamp1 = "2022-01-03 18:30:15";
        let generic: chrono::NaiveDateTime = GenericWrapper::from(timestamp1).try_into().unwrap();
        assert_eq!(
            timestamp1,
            generic.to_string(),
            "Timestamp strings don't match"
        );

        let timestamp2 = "2022-01-03T18:30:15";
        let generic: chrono::NaiveDateTime = GenericWrapper::from(timestamp2).try_into().unwrap();
        assert_eq!(
            timestamp1,
            generic.to_string(),
            "Timestamp strings don't match"
        );

        let json_object = r#"   { "col1": "valoare",
            "col2": 2345,
            "col3": "2022-01-03 18:30:15"} 
        "#;
        let generic: serde_json::Value = GenericWrapper::from(json_object).try_into().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json_object).unwrap(),
            generic,
            "Json variables don't match"
        );

        let json_array = r#"  [
                "val1",
                1,
                "val2",
                100.8755
            ]
        "#;
        let generic: serde_json::Value = GenericWrapper::from(json_array).try_into().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json_array).unwrap(),
            generic,
            "Json variables don't match"
        );

        let json_array = r#"  [
                { "col1": "valoare", "col2": 2345, "col3": "2022-01-03 18:30:15"},
                { "col1": "valoare", "col2": 100.13432, "col3": "2022-01-03 18:30:15"},
                { "col1": "valoare", "col2": 2345, "col3": true},
                { "col1": "valoare", "col2": 2345, "col3": "2022-01-03 18:30:15"}
            ]
        "#;
        let generic: serde_json::Value = GenericWrapper::from(json_array).try_into().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json_array).unwrap(),
            generic,
            "Json variables don't match"
        );
    }

    #[test]
    fn wrapper_serde() {
        let uuid = uuid::Uuid::new_v4();
        let timestamp = chrono::NaiveDateTime::parse_from_str(
            "2022-01-03T18:30:15",
            super::CHRONO_TMSTAMP_ISO_FORMAT,
        )
        .unwrap();
        let json = serde_json::from_str::<serde_json::Value>(
            r#"{ "col1": "valoare", "col2": 2345, "col3": "2022-01-03 18:30:15"}"#,
        )
        .unwrap();
        let vec_ser = vec![
            GenericWrapper::String("start".to_string()),
            GenericWrapper::None,
            GenericWrapper::NaiveTimestamp(timestamp),
            GenericWrapper::Uuid(uuid),
            GenericWrapper::Json(json),
        ];
        let res = serde_json::to_string(&vec_ser).unwrap();
        println!("\nSerialized: {res}");

        let vec_des: Vec<GenericWrapper> = serde_json::from_str(&res).unwrap();
        assert_eq!(
            TryInto::<chrono::NaiveDateTime>::try_into(vec_ser.get(2).unwrap().to_owned()).unwrap(),
            TryInto::<chrono::NaiveDateTime>::try_into(vec_des.get(2).unwrap().to_owned()).unwrap()
        );
    }

    #[test]
    fn generic_row_serde() {
        let mut map1: IndexMap<&str, GenericWrapper> = IndexMap::new();
        map1.insert(
            "col1",
            GenericWrapper::from(
                chrono::NaiveDateTime::parse_from_str(
                    "2015-09-05 23:56:04",
                    super::CHRONO_TMSTAMP_ANSI_FORMAT,
                )
                .unwrap()
                .to_string(),
            ),
        );
        map1.insert("col2", GenericWrapper::from(uuid::Uuid::new_v4()));
        map1.insert("col3", GenericWrapper::None);
        map1.insert("col4", GenericWrapper::from(1));
        map1.insert("col5", GenericWrapper::from(1.0));
        map1.insert("col6", GenericWrapper::Json(serde_json::from_str(r#"[{"fld1":"test"},{"fld2":100.0},{"fld3":
                                                                                {"val1":true,"val2":null},"fld4":"2022-01-01"}]"#).unwrap()));
        map1.insert("col7", GenericWrapper::from(r#"testare"#));

        let json = serde_json::to_string(&GenericSqlRow::from(&map1)).unwrap();
        println!("\nJSON: {}", json);
        println!(
            "JSON Value {}",
            serde_json::to_string(map1.get(&"col7").unwrap()).unwrap()
        );
        let map2: GenericSqlRow<&str, GenericWrapper> =
            serde_json::from_str(json.as_str()).unwrap();
        assert_eq!(
            map1.get(&"col6").unwrap(),
            map2.as_ref().get(&"col6").unwrap()
        );
    }
}
