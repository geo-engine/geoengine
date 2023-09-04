#[cfg(not(feature = "pro"))] // TODO: this needs to be done without mentioning pro
pub mod apidoc;
mod doc;
pub mod handlers;
pub mod model;
pub mod ogc;
