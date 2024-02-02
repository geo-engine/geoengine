pub mod adapters;
#[macro_use]
pub mod engine;
pub mod error;
pub mod mock;
pub mod plot;
pub mod processing;
pub mod source;
pub mod util;

/// Compiles Geo Engine Pro
#[cfg(feature = "pro")]
pub mod pro;

use geoengine_datatypes::test_data;
