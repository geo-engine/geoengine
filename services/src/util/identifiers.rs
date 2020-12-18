use serde::{Deserialize, Serialize};

pub trait Identifier: Sized {
    /// Create a new (random) identifier
    fn new() -> Self;
}

#[macro_export]
macro_rules! identifier {
    ($id_name: ident) => {
        #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Clone, Copy, Hash)]
        pub struct $id_name(pub uuid::Uuid);

        impl crate::util::identifiers::Identifier for $id_name {
            fn new() -> Self {
                Self(uuid::Uuid::new_v4())
            }
        }

        impl std::fmt::Display for $id_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl std::str::FromStr for $id_name {
            type Err = crate::error::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(
                    uuid::Uuid::from_str(s).map_err(|_error| crate::error::Error::InvalidUuid)?,
                ))
            }
        }

        impl<'a> postgres_types::FromSql<'a> for $id_name {
            fn from_sql(
                ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                uuid::Uuid::from_sql(ty, raw).map(Self)
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                <uuid::Uuid as postgres_types::FromSql>::accepts(ty)
            }
        }

        impl postgres_types::ToSql for $id_name {
            fn to_sql(
                &self,
                ty: &postgres_types::Type,
                out: &mut postgres_types::private::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
            where
                Self: Sized,
            {
                self.0.to_sql(ty, out)
            }

            fn to_sql_checked(
                &self,
                ty: &postgres_types::Type,
                out: &mut postgres_types::private::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                self.0.to_sql_checked(ty, out)
            }

            fn accepts(ty: &postgres_types::Type) -> bool
            where
                Self: Sized,
            {
                <uuid::Uuid as postgres_types::ToSql>::accepts(ty)
            }
        }
    };
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct IdResponse<T: Identifier> {
    pub id: T,
}

impl<T> IdResponse<T>
where
    T: Identifier,
{
    pub fn from_id(id: T) -> Self {
        Self { id }
    }
}
