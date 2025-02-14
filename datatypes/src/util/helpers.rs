use rayon::iter::{
    plumbing::Producer, IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};

/// This macro allows comparing float slices using [`float_cmp::approx_eq`].
#[macro_export]
macro_rules! assert_approx_eq {
    ($left:expr, $right:expr $(,)?) => ({
        if !$crate::util::helpers::approx_eq_floats($left, $right) {
            panic!("assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`", $left, $right)
        }
    });
    ($left:expr, $right:expr, $($arg:tt)+) => ({
        if !$crate::util::helpers::approx_eq_floats($left, $right) {
            panic!("assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`: {}", $left, $right,
                           format_args!($($arg)+))
        }
    });
}

#[must_use]
pub fn approx_eq_floats(left: &[f64], right: &[f64]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    for (&l, &r) in left.iter().zip(right) {
        if !float_cmp::approx_eq!(f64, l, r) {
            return false;
        }
    }

    true
}

/// Create hash maps by specifying key-value pairs
#[macro_export]
macro_rules! hashmap {
    (@void $($x:tt)*) => (());
    (@count $($tts:expr),*) => (<[()]>::len(&[$(hashmap!(@void $tts)),*]));

    ($($key:expr => $value:expr,)+) => { hashmap!($($key => $value),+) };
    ($($key:expr => $value:expr),*) => {
        {
            let capacity = hashmap!(@count $($key),*);
            let mut map = ::std::collections::HashMap::with_capacity(capacity);
            $(
                let _ = map.insert($key, $value);
            )*
            map
        }
    };
}

/// Converts an `Iterator<Item=T>` to an `Iterator<Item=Option<T>>`
#[derive(Clone, Debug)]
pub struct SomeIter<I, T>
where
    I: Iterator<Item = T>,
{
    inner_iterator: I,
}

impl<I, T> SomeIter<I, T>
where
    I: Iterator<Item = T>,
{
    pub fn new(iterator: I) -> Self {
        Self {
            inner_iterator: iterator,
        }
    }
}

impl<I, T> Iterator for SomeIter<I, T>
where
    I: Iterator<Item = T>,
{
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner_iterator.next().map(Into::into)
    }
}

impl<I, T> DoubleEndedIterator for SomeIter<I, T>
where
    I: Iterator<Item = T> + DoubleEndedIterator,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner_iterator.next_back().map(Into::into)
    }
}

impl<I, T> ExactSizeIterator for SomeIter<I, T> where I: Iterator<Item = T> + ExactSizeIterator {}

/// Converts an `ParallelIterator<Item=T>` to an `ParallelIterator<Item=Option<T>>`
#[derive(Clone, Debug)]
pub struct SomeParIter<I, T>
where
    I: Iterator<Item = T>,
{
    inner_iterator: SomeIter<I, T>,
}

impl<I, T> SomeParIter<I, T>
where
    I: Iterator<Item = T>,
{
    pub fn new(iterator: SomeIter<I, T>) -> Self {
        Self {
            inner_iterator: iterator,
        }
    }
}

impl<I, T> IntoParallelIterator for SomeIter<I, T>
where
    I: Iterator<Item = T> + ExactSizeIterator + DoubleEndedIterator + Producer + Send,
    T: Send,
{
    type Item = Option<T>;
    type Iter = SomeParIter<I, T>;

    fn into_par_iter(self) -> Self::Iter {
        SomeParIter::new(self)
    }
}

impl<I, T> ParallelIterator for SomeParIter<I, T>
where
    I: Iterator<Item = T> + ExactSizeIterator + DoubleEndedIterator + Producer + Send,
    T: Send,
{
    type Item = Option<T>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
    {
        rayon::iter::plumbing::bridge(self, consumer)
    }
}

impl<I, T> Producer for SomeIter<I, T>
where
    I: Iterator<Item = T> + ExactSizeIterator + DoubleEndedIterator + Producer + Send,
    T: Send,
{
    type Item = Option<T>;

    type IntoIter = SomeIter<I, T>;

    fn into_iter(self) -> Self::IntoIter {
        Self::new(self.inner_iterator)
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.inner_iterator.split_at(index);
        (Self::new(left), Self::new(right))
    }
}

impl<I, T> Producer for SomeParIter<I, T>
where
    I: Iterator<Item = T> + ExactSizeIterator + DoubleEndedIterator + Producer + Send,
    T: Send,
{
    type Item = Option<T>;

    type IntoIter = SomeIter<I, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner_iterator
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.inner_iterator.split_at(index);
        (SomeParIter::new(left), SomeParIter::new(right))
    }
}

impl<I, T> IndexedParallelIterator for SomeParIter<I, T>
where
    I: Iterator<Item = T> + ExactSizeIterator + DoubleEndedIterator + Producer + Send,
    T: Send,
{
    fn len(&self) -> usize {
        self.inner_iterator.inner_iterator.len()
    }

    fn drive<C: rayon::iter::plumbing::Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        rayon::iter::plumbing::bridge(self, consumer)
    }

    fn with_producer<CB: rayon::iter::plumbing::ProducerCallback<Self::Item>>(
        self,
        callback: CB,
    ) -> CB::Output {
        callback.callback(self)
    }
}

/// snap `value` to previous `step` multiple from `start`
pub fn snap_prev(start: f64, step: f64, value: f64) -> f64 {
    start + ((value - start) / step).floor() * step
}

/// snap `value` to next `step` multiple from `start`
pub fn snap_next(start: f64, step: f64, value: f64) -> f64 {
    start + ((value - start) / step).ceil() * step
}

/// test if a vlue is equal to another ot if both are NAN
#[allow(clippy::eq_op)]
#[inline]
pub fn equals_or_both_nan<T: PartialEq>(value: &T, no_data_value: &T) -> bool {
    value == no_data_value || (value != value && no_data_value != no_data_value)
}

/// Helper function for `split_at` of a `rayon::iter::plumbing::Producer`.
///
/// It returns `(left_index, left_length_right_index, right_length)`.
pub fn indices_for_split_at(
    index: usize,
    length: usize,
    split_index: usize,
) -> (usize, usize, usize) {
    // Example:
    //   Index: 0, Length 3
    //   Split at 1
    //   Left: Index: 0, Length: 1 -> Elements: 0
    //   Right: Index: 1, Length: 3 -> Elements: 1, 2

    debug_assert!(index + split_index <= length, "split index out of bounds");

    let left_index = index;
    let left_length_right_index = left_index + split_index;
    let right_length = length;

    (left_index, left_length_right_index, right_length)
}

pub fn ge_report<E: snafu::Error>(error: E) -> String {
    //newline same on every os for report
    const NOTE_MARKER: &str = "\nNOTE:";
    const REDUNDANCY_MARKER: &str = " *\n";

    let full = snafu::Report::from_error(error).to_string();
    //split once from end to skip potential markers inside messages
    if let Some(note_pos) = full.rfind(NOTE_MARKER) {
        let message = &full[0..note_pos];
        message.replace(REDUNDANCY_MARKER, "\n")
    } else {
        full
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{collections::FeatureCollectionError, error::Error};

    use super::*;

    #[test]
    fn assert_approx_eq_for_floats() {
        assert_approx_eq!(&[1., 2., 3.], &[1., 2., 3.]);

        assert!(!approx_eq_floats(&[1., 2.], &[1., 2., 3.]));
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_snaps_right() {
        assert_eq!(snap_next(1., 2., 4.5), 5.);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_snaps_left() {
        assert_eq!(snap_prev(1., 2., 4.5), 3.);
    }

    #[test]
    fn create_hash_map() {
        let _single: HashMap<&str, u32> = hashmap!(
            "foo" => 23,
        );

        let _multiple: HashMap<&str, u32> = hashmap!(
            "foo" => 23,
            "bar" => 42,
        );
    }

    #[test]
    fn no_data_value_false() {
        let value = 1;
        let no_data_value = 0;

        assert!(!equals_or_both_nan(&value, &no_data_value));
    }

    #[test]
    fn no_data_value_true() {
        let value = 1;
        let no_data_value = 1;

        assert!(equals_or_both_nan(&value, &no_data_value));
    }

    #[test]
    fn no_data_value_nan_false() {
        let value = 1.;
        let no_data_value = f32::NAN;

        assert!(!equals_or_both_nan(&value, &no_data_value));
    }

    #[test]
    fn no_data_value_nan_true() {
        let value = f32::NAN;
        let no_data_value = f32::NAN;

        assert!(equals_or_both_nan(&value, &no_data_value));
    }

    #[test]
    fn no_data_value_option_false() {
        let value = Some(1);
        let no_data_value = Some(0);

        assert!(!equals_or_both_nan(&value, &no_data_value));
    }

    #[test]
    fn no_data_value_option_true() {
        let value = Some(1);
        let no_data_value = Some(1);

        assert!(equals_or_both_nan(&value, &no_data_value));
    }

    #[test]
    fn no_data_value_option_nan_false() {
        let value = Some(1.);
        let no_data_value = Some(f32::NAN);

        assert!(!equals_or_both_nan(&value, &no_data_value));
    }

    #[test]
    fn no_data_value_option_nan_true() {
        let value = Some(f32::NAN);
        let no_data_value = Some(f32::NAN);

        assert!(equals_or_both_nan(&value, &no_data_value));
    }

    #[test]
    fn split_at_helper() {
        let (left_index, left_length_right_index, right_length) = indices_for_split_at(0, 3, 1);
        assert_eq!((left_index, left_length_right_index), (0, 1));
        assert_eq!((left_length_right_index, right_length), (1, 3));
    }

    #[test]
    fn it_removes_note_from_report() {
        assert_eq!(
            std::env::var("SNAFU_RAW_ERROR_MESSAGES"),
            Err(std::env::VarError::NotPresent),
            "Precondition failed"
        );
        let error = Error::FeatureCollection {
            source: FeatureCollectionError::EmptyPredicate,
        };
        assert_eq!(
            snafu::Report::from_error(&error).to_string(),
            r"Feature collection error *

Caused by this error:
  1: EmptyPredicate

NOTE: Some redundant information has been removed from the lines marked with *. Set SNAFU_RAW_ERROR_MESSAGES=1 to disable this behavior.
",
            "Precondition failed"
        );

        assert_eq!(
            ge_report(error),
            r"Feature collection error

Caused by this error:
  1: EmptyPredicate
"
        );
    }
}
