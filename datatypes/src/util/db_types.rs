use std::collections::HashMap;

use ordered_float::NotNan;
use postgres_types::{FromSql, ToSql};

/// A macro for quickly implementing `FromSql` and `ToSql` for `$RustType` if there is a `From` and `Into`
/// implementation for another type `$DbType` that already implements it.
///
/// # Usage
///
/// ```rust,ignore
/// delegate_from_to_sql!($RustType, $DbType)
/// ```
///
#[macro_export]
macro_rules! delegate_from_to_sql {
    ( $RustType:ty, $DbType:ty ) => {
        impl ToSql for $RustType {
            fn to_sql(
                &self,
                ty: &postgres_types::Type,
                w: &mut bytes::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <$DbType as ToSql>::to_sql(&self.into(), ty, w)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <$DbType as FromSql>::accepts(ty)
            }

            postgres_types::to_sql_checked!();
        }

        impl<'a> FromSql<'a> for $RustType {
            fn from_sql(
                ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<$RustType, Box<dyn std::error::Error + Sync + Send>> {
                Ok(<$DbType as FromSql>::from_sql(ty, raw)?.try_into()?)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <$DbType as FromSql>::accepts(ty)
            }
        }
    };
}

#[derive(Debug, PartialEq, ToSql, FromSql)]
pub struct TextTextKeyValue {
    key: String,
    value: String,
}

#[derive(Debug, PartialEq, ToSql, FromSql)]
#[postgres(transparent)]
pub struct HashMapTextTextDbType(pub Vec<TextTextKeyValue>);

impl From<&HashMap<String, String>> for HashMapTextTextDbType {
    fn from(map: &HashMap<String, String>) -> Self {
        Self(
            map.iter()
                .map(|(key, value)| TextTextKeyValue {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        )
    }
}

impl<S: std::hash::BuildHasher + std::default::Default> From<HashMapTextTextDbType>
    for HashMap<String, String, S>
{
    fn from(map: HashMapTextTextDbType) -> Self {
        map.0
            .into_iter()
            .map(|TextTextKeyValue { key, value }| (key, value))
            .collect()
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct StringPair((String, String));

impl StringPair {
    pub fn new(a: String, b: String) -> Self {
        Self((a, b))
    }

    pub fn into_inner(self) -> (String, String) {
        self.0
    }
}

impl ToSql for StringPair {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // unpack domain type
        let ty = match ty.kind() {
            postgres_types::Kind::Domain(ty) => ty,
            _ => ty,
        };

        let (a, b) = &self.0;
        <[&String; 2] as ToSql>::to_sql(&[a, b], ty, w)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        // unpack domain type
        let ty = match ty.kind() {
            postgres_types::Kind::Domain(ty) => ty,
            _ => ty,
        };

        <[String; 2] as ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for StringPair {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // unpack domain type
        let ty = match ty.kind() {
            postgres_types::Kind::Domain(ty) => ty,
            _ => ty,
        };

        let [a, b] = <[String; 2] as FromSql>::from_sql(ty, raw)?;

        Ok(Self((a, b)))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        // unpack domain type
        let ty = match ty.kind() {
            postgres_types::Kind::Domain(ty) => ty,
            _ => ty,
        };

        <[String; 2] as FromSql>::accepts(ty)
    }
}

impl From<(String, String)> for StringPair {
    fn from(value: (String, String)) -> Self {
        Self(value)
    }
}

impl From<StringPair> for (String, String) {
    fn from(value: StringPair) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotNanF64(NotNan<f64>);

impl ToSql for NotNanF64 {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <f64 as ToSql>::to_sql(&self.0.into_inner(), ty, w)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <f64 as ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for NotNanF64 {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<NotNanF64, Box<dyn std::error::Error + Sync + Send>> {
        let value = <f64 as FromSql>::from_sql(ty, raw)?;

        Ok(NotNanF64(value.try_into()?))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <f64 as FromSql>::accepts(ty)
    }
}

impl From<NotNan<f64>> for NotNanF64 {
    fn from(value: NotNan<f64>) -> Self {
        Self(value)
    }
}

impl From<NotNanF64> for NotNan<f64> {
    fn from(value: NotNanF64) -> Self {
        value.0
    }
}
