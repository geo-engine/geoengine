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
    };
}
