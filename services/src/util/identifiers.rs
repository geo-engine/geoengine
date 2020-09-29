pub trait Identifier {
    /// Create a new (random) identifier
    fn new() -> Self;

    /// Create identifier from given `id`
    fn from_uuid(id: uuid::Uuid) -> Self;

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
