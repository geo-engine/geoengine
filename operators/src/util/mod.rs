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
use std::collections::HashSet;
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

#[derive(Debug, Clone, PartialEq)]
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
