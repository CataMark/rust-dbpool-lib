use crate::error::ErrorReport;

type PGSQLType = postgres_types::Type;

impl postgres_types::ToSql for super::GenericWrapper {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut postgres_types::private::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            Self::None => Ok(postgres_types::IsNull::Yes),
            Self::Bool(v) => v.to_sql(ty, out),
            Self::I8(v) => v.to_sql(ty, out),
            Self::U8(v) => (*v as u32).to_sql(ty, out),
            Self::I16(v) => v.to_sql(ty, out),
            Self::U16(v) => (*v as u32).to_sql(ty, out),
            Self::I32(v) => v.to_sql(ty, out),
            Self::U32(v) => v.to_sql(ty, out),
            Self::I64(v) => v.to_sql(ty, out),
            Self::F32(v) => v.to_sql(ty, out),
            Self::F64(v) => v.to_sql(ty, out),
            Self::Char(v) => v.to_string().to_sql(ty, out),
            Self::String(v) => v.to_sql(ty, out),
            Self::BinVec(v) => v.to_sql(ty, out),
            Self::Uuid(v) => v.to_sql(ty, out),
            Self::Decimal(v) => v.to_sql(ty, out),
            Self::NaiveDate(v) => v.to_sql(ty, out),
            Self::NaiveTime(v) => v.to_sql(ty, out),
            Self::NaiveTimestamp(v) => v.to_sql(ty, out),
            Self::Json(v) => v.to_sql(ty, out),
            _ => Err(self.into_error("pgsql").into()),
        }
    }

    fn accepts(ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        matches!(
            *ty,
            PGSQLType::BOOL
                | PGSQLType::CHAR
                | PGSQLType::VARCHAR
                | PGSQLType::TEXT
                | PGSQLType::NAME
                | PGSQLType::BYTEA
                | PGSQLType::OID
                | PGSQLType::INT2
                | PGSQLType::INT4
                | PGSQLType::INT8
                | PGSQLType::FLOAT4
                | PGSQLType::FLOAT8
                | PGSQLType::NUMERIC
                | PGSQLType::UUID
                | PGSQLType::JSON
                | PGSQLType::JSONB
                | PGSQLType::DATE
                | PGSQLType::TIME
                | PGSQLType::TIMESTAMP
        )
    }

    fn to_sql_checked(
        &self,
        ty: &postgres_types::Type,
        out: &mut postgres_types::private::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Self::None => Ok(postgres_types::IsNull::Yes),
            Self::Bool(v) => v.to_sql_checked(ty, out),
            Self::I8(v) => v.to_sql_checked(ty, out),
            Self::U8(v) => (*v as u32).to_sql_checked(ty, out),
            Self::I16(v) => v.to_sql_checked(ty, out),
            Self::U16(v) => (*v as u32).to_sql_checked(ty, out),
            Self::I32(v) => v.to_sql_checked(ty, out),
            Self::U32(v) => v.to_sql_checked(ty, out),
            Self::I64(v) => v.to_sql_checked(ty, out),
            Self::F32(v) => v.to_sql_checked(ty, out),
            Self::F64(v) => v.to_sql_checked(ty, out),
            Self::Char(v) => v.to_string().to_sql_checked(ty, out),
            Self::String(v) => v.to_sql_checked(ty, out),
            Self::BinVec(v) => v.to_sql_checked(ty, out),
            Self::Uuid(v) => v.to_sql_checked(ty, out),
            Self::Decimal(v) => v.to_sql_checked(ty, out),
            Self::NaiveDate(v) => v.to_sql_checked(ty, out),
            Self::NaiveTime(v) => v.to_sql_checked(ty, out),
            Self::NaiveTimestamp(v) => v.to_sql_checked(ty, out),
            Self::Json(v) => v.to_sql_checked(ty, out),
            _ => Err(self.into_error("pgsql").into()),
        }
    }
}

impl<'a> postgres_types::FromSql<'a> for super::GenericWrapper {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            PGSQLType::BOOL => Ok(Self::Bool(bool::from_sql(ty, raw)?)),
            PGSQLType::CHAR => Ok(Self::String(String::from_sql(ty, raw)?)),
            PGSQLType::VARCHAR => Ok(Self::String(String::from_sql(ty, raw)?)),
            PGSQLType::TEXT => Ok(Self::String(String::from_sql(ty, raw)?)),
            PGSQLType::NAME => Ok(Self::String(String::from_sql(ty, raw)?)),
            PGSQLType::BYTEA => Ok(Self::BinVec(
                <Vec<u8> as postgres_types::FromSql>::from_sql(ty, raw)?,
            )),
            PGSQLType::OID => Ok(Self::U32(u32::from_sql(ty, raw)?)),
            PGSQLType::INT2 => Ok(Self::I16(i16::from_sql(ty, raw)?)),
            PGSQLType::INT4 => Ok(Self::I32(i32::from_sql(ty, raw)?)),
            PGSQLType::INT8 => Ok(Self::I64(i64::from_sql(ty, raw)?)),
            PGSQLType::FLOAT4 => Ok(Self::F32(f32::from_sql(ty, raw)?)),
            PGSQLType::FLOAT8 => Ok(Self::F64(f64::from_sql(ty, raw)?)),
            PGSQLType::NUMERIC => Ok(Self::Decimal(rust_decimal::Decimal::from_sql(ty, raw)?)),
            PGSQLType::UUID => Ok(Self::Uuid(uuid::Uuid::from_sql(ty, raw)?)),
            PGSQLType::JSON => Ok(Self::Json(serde_json::Value::from_sql(ty, raw)?)),
            PGSQLType::JSONB => Ok(Self::Json(serde_json::Value::from_sql(ty, raw)?)),
            PGSQLType::DATE => Ok(Self::NaiveDate(chrono::NaiveDate::from_sql(ty, raw)?)),
            PGSQLType::TIME => Ok(Self::NaiveTime(chrono::NaiveTime::from_sql(ty, raw)?)),
            PGSQLType::TIMESTAMP => Ok(Self::NaiveTimestamp(chrono::NaiveDateTime::from_sql(
                ty, raw,
            )?)),
            _ => Err("There is no conversion implemented for postgres type datetimeoffset".into()),
        }
    }

    fn from_sql_null(
        _: &postgres_types::Type,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self::None)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <Self as postgres_types::ToSql>::accepts(ty)
    }
}

impl super::FieldMetadata {
    pub fn pgsql_column_name(&self) -> String {
        format!(r#""{}""#, self.name)
    }

    pub fn pgsql_type_name(&self) -> String {
        if let Some(val) = self.character_maximum_length {
            return format!("{}({})", self.udt_name, val);
        }

        if Some(10) == self.numeric_precision_radix
            && self.numeric_precision.is_some()
            && self.numeric_scale.is_some()
        {
            return format!(
                "{}({}, {})",
                self.udt_name,
                self.numeric_precision.unwrap(),
                self.numeric_scale.unwrap()
            );
        }

        self.udt_name.to_owned()
    }
}

impl TryFrom<tokio_postgres::Row> for super::FieldMetadata {
    type Error = ErrorReport;

    fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            name: row.try_get("column_name")?,
            character_maximum_length: row.try_get("character_maximum_length")?,
            numeric_precision: row.try_get("numeric_precision")?,
            numeric_precision_radix: row.try_get("numeric_precision_radix")?,
            numeric_scale: row.try_get("numeric_scale")?,
            udt_name: row.try_get("udt_name")?,
            _oid: row.try_get("oid")?,
        })
    }
}

impl super::FieldMetadata {
    pub fn pgsql_type(&self) -> Result<postgres_types::Type, ErrorReport> {
        match postgres_types::Type::from_oid(self._oid) {
            Some(val) => Ok(val),
            None => Err(crate::error::error_pgsql_type_conversion(
                &self.udt_name,
                &self._oid,
            )),
        }
    }
}

impl TryFrom<tokio_postgres::Row> for super::GenericSqlRow<String, super::GenericWrapper> {
    type Error = ErrorReport;

    fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
        let mut res = indexmap::IndexMap::new();
        for i in 0..row.len() {
            res.insert(row.columns()[i].name().into(), row.try_get(i)?);
        }
        Ok(Self(res))
    }
}
