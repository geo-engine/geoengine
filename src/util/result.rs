use std::error::Error;

// TODO: plus 'static?
pub type Result<T, E = Box<dyn Error + Send + Sync>> = std::result::Result<T, E>;
