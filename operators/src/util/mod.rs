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

/// Remove leading spaces from each line equally
pub fn unindent(string: &str) -> String {
    /// Count leading spaces, return `usize::MAX` if only spaces
    fn count_spaces(line: &str) -> usize {
        for (i, c) in line.chars().enumerate() {
            if c != ' ' {
                return i;
            }
        }

        usize::MAX
    }

    let num_spaces = string.lines().skip(1).map(count_spaces).min().unwrap_or(0);

    string
        .lines()
        .enumerate()
        .fold(String::new(), |mut acc, (i, line)| {
            if i > 0 {
                acc.push('\n');
            }

            if i == 0 {
                // don't unindent line that contains opening quote
                acc.push_str(line);
            } else if line.len() > num_spaces {
                acc.push_str(&line[num_spaces..]);
            }

            acc
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unindent() {
        assert_eq!(
            unindent(
                r#"foo
                   bar
                "#
            ),
            "foo\nbar\n"
        );
    }
}
