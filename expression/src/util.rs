use std::collections::HashSet;

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
