mod async_util;
pub mod gdal;
mod gdal_config_options;
pub mod input;
pub mod math;
pub mod number_statistics;
pub mod raster_stream_to_geotiff;
pub mod raster_stream_to_png;
mod rayon;
pub mod retry;
pub mod statistics;
pub mod stream_zip;
pub mod string_token;
pub mod sunpos;
pub mod test;
mod wrap_with_projection_and_resample;

use crate::error::Error;
use std::collections::HashSet;
use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub use self::async_util::{
    QUERY_TASK_CHANNEL_CAPACITY, abortable_query_execution, spawn, spawn_blocking,
    spawn_blocking_with_thread_pool, spawn_query, spawn_query_task, spawn_stream,
};
pub use self::gdal_config_options::{GdalConfigOptions, TemporaryGdalThreadLocalConfigOptions};
pub use self::rayon::create_rayon_thread_pool;
pub use wrap_with_projection_and_resample::WrapWithProjectionAndResample;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Get a lock for mutex and recover from poisoning
/// TODO: proper poisoning handling
pub fn safe_lock_mutex<T: ?Sized>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Get a read lock for a `RwLock` and recover from poisoning, like [`safe_lock_mutex`]
pub fn safe_read_rwlock<T: ?Sized>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

/// Get a write lock for a `RwLock` and recover from poisoning, like [`safe_lock_mutex`]
pub fn safe_write_rwlock<T: ?Sized>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuplicateOrEmpty {
    Ok,
    Duplicate(String),
    Empty,
}

/// Checks if a string is empty or duplicated within a slice
pub fn duplicate_or_empty_str_slice<S: AsRef<str>>(strings: &[S]) -> DuplicateOrEmpty {
    let mut set = HashSet::new();

    for string in strings {
        let string = string.as_ref();

        if string.is_empty() {
            return DuplicateOrEmpty::Empty;
        }

        if !set.insert(string) {
            return DuplicateOrEmpty::Duplicate(string.to_string());
        }
    }

    DuplicateOrEmpty::Ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duplicate_or_empty_str_slice() {
        assert_eq!(
            duplicate_or_empty_str_slice(&["a", "b", "c"]),
            DuplicateOrEmpty::Ok
        );

        assert_eq!(
            duplicate_or_empty_str_slice(&["a", "", "c"]),
            DuplicateOrEmpty::Empty
        );

        assert_eq!(
            duplicate_or_empty_str_slice(&["a", "a", "c"]),
            DuplicateOrEmpty::Duplicate("a".to_string())
        );
    }
}
