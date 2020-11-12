use serde::{Deserialize, Serialize};

pub trait Identifier: Sized {
    /// Create a new (random) identifier
    fn new() -> Self;
}

#[macro_export]
macro_rules! identifier {
    ($id_name: ident) => {
        #[derive(
            Debug,
            PartialEq,
            Eq,
            serde::Serialize,
            serde::Deserialize,
            Clone,
            Copy,
            Hash,
            postgres_types::FromSql,
            postgres_types::ToSql,
        )]
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
