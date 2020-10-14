use crate::error;
use crate::error::Result;
use std::str::FromStr;

pub trait Identifier: Sized {
    /// Create a new (random) identifier
    fn new() -> Self;

    /// Create identifier from given `id`
    fn from_uuid(id: uuid::Uuid) -> Self;

    /// Create identifier from given `id`
    fn from_uuid_str(uuid_str: &str) -> Result<Self> {
        Ok(Self::from_uuid(
            uuid::Uuid::from_str(uuid_str).map_err(|_| error::Error::InvalidUuid)?,
        ))
    }

    /// Get the internal uuid
    fn uuid(&self) -> uuid::Uuid;
}

#[macro_export]
macro_rules! identifier {
    ($id_name: ident) => {
        #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Clone, Copy, Hash)]
        pub struct $id_name {
            id: uuid::Uuid,
        }

        impl crate::util::identifiers::Identifier for $id_name {
            fn from_uuid(id: uuid::Uuid) -> Self {
                Self { id }
            }

            fn new() -> Self {
                Self {
                    id: uuid::Uuid::new_v4(),
                }
            }

            fn uuid(&self) -> uuid::Uuid {
                self.id
            }
        }

        impl std::fmt::Display for $id_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.id)
            }
        }

        impl<'a> postgres_types::FromSql<'a> for $id_name {
            fn from_sql(
                ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                uuid::Uuid::from_sql(ty, raw).map(|id| Self { id })
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
                self.id.to_sql(ty, out)
            }

            fn accepts(ty: &postgres_types::Type) -> bool
            where
                Self: Sized,
            {
                <uuid::Uuid as postgres_types::ToSql>::accepts(ty)
            }

            fn to_sql_checked(
                &self,
                ty: &postgres_types::Type,
                out: &mut postgres_types::private::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                self.id.to_sql_checked(ty, out)
            }
        }
    };
}
