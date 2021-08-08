use std::cmp::{max, min};

pub fn value_in_range<T>(value: T, min: T, max: T) -> bool
where
    T: PartialOrd + Copy,
{
    (value >= min) && (value < max)
}

pub fn value_in_range_inclusive<T>(value: T, min: T, max: T) -> bool
where
    T: PartialOrd + Copy,
{
    (value >= min) && (value <= max)
}

pub fn value_in_range_inv<T>(value: T, min: T, max: T) -> bool
where
    T: PartialOrd + Copy,
{
    (value > min) && (value <= max)
}

pub fn overlap_inclusive<T: Copy + PartialOrd + Ord>(a: (T, T), b: (T, T)) -> Option<(T, T)> {
    if value_in_range_inclusive(a.0, b.0, b.1) || value_in_range_inclusive(b.0, a.0, a.1) {
        let start = max(a.0, b.0);
        let end = min(a.1, b.1);
        return Some((start, end));
    }
    None
}
