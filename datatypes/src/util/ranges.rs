pub fn value_in_range<T>(value: T, min: T, max: T) -> bool
where
    T: PartialOrd + Copy,
{
    (value >= min) && (value <= max)
}
