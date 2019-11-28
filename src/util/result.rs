use std::error::Error;

// TODO: remove 'static?
pub type Result<T, E = Box<dyn Error + Send + Sync + 'static>> = std::result::Result<T, E>;
