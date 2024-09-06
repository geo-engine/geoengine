pub mod api;
pub mod contexts;
pub mod datasets;
pub mod error;
pub mod layers;
pub mod machine_learning;
pub mod projects;
#[cfg(not(feature = "pro"))]
pub mod server;
pub mod stac;
#[macro_use]
pub mod util;
pub mod tasks;
pub mod workflows;

/// Compiles Geo Engine Pro
#[cfg(feature = "pro")]
pub mod pro;

pub use geoengine_datatypes::test_data;

// re-export test macro
pub mod ge_context {
    pub use geoengine_macros::test;
}
