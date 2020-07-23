pub fn value_in_range<T>(value: T, min: T, max: T) -> bool
where
    T: PartialOrd,
{
    (value >= min) && (value <= max)
}
