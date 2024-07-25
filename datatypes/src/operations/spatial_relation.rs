/// This method returns `true` iff `Self` contains the input `T`.
/// It is valid if the `T` touches the `Self`'s borders.
pub trait Contains<T> {
    fn contains(&self, other: &T) -> bool;
}
