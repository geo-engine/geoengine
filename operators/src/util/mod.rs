pub mod gdal;
pub mod input;
pub mod math;
pub mod number_statistics;
pub mod raster_stream_to_geotiff;
pub mod raster_stream_to_png;
mod rayon;
pub mod statistics;
pub mod string_token;
pub mod sunpos;

use crate::error::Error;
use std::ops::Deref;
use std::sync::{Mutex, MutexGuard};

pub use self::rayon::create_rayon_thread_pool;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Get a lock for mutex and recover from poisoning
/// TODO: proper poisoning handling
pub fn safe_lock_mutex<M, T>(lock: &M) -> MutexGuard<T>
where
    M: Deref<Target = Mutex<T>>,
{
    match lock.deref().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
