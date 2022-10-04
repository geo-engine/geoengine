/// This macro allows comparing float slices using [`float_cmp::approx_eq`].
#[macro_export]
macro_rules! assert_approx_eq {
    ($left:expr, $right:expr $(,)?) => ({
        if !$crate::util::helpers::approx_eq_floats($left, $right) {
            panic!(r#"assertion failed: `(left == right)`
            left: `{:?}`,
           right: `{:?}`"#, $left, $right)
        }
    });
    ($left:expr, $right:expr, $($arg:tt)+) => ({
        if !$crate::util::helpers::approx_eq_floats($left, $right) {
            panic!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`: {}"#, $left, $right,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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
}
