#[cfg(not(feature = "pro"))] // TODO: this needs to be done without mentioning pro
pub mod apidoc;
pub mod cli;
pub mod handlers;
pub mod model;
pub mod ogc;
