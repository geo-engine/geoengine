/// This macro allows comparing float slices using [float_cmp::approx_eq].
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_approx_eq_for_floats() {
        assert_approx_eq!(&[1., 2., 3.], &[1., 2., 3.]);

        assert!(!approx_eq_floats(&[1., 2.], &[1., 2., 3.]));
    }
}
