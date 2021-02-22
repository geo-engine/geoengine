/// This macro allows specifying a JSON Map inplace for convenience.
///
/// The code is taken and modified from maplit (https://github.com/bluss/maplit/blob/master/src/lib.rs).
///
/// # Examples
///
/// ```
/// use serde_json::{Map, Value};
/// use geoengine_datatypes::json_map;
///
/// let mut map: Map<String, Value> = Map::with_capacity(2);
/// let _ = map.insert("foo".to_string(), 42.into());
/// let _ = map.insert("bar".to_string(), 1337.into());
///
/// assert_eq!(
///     map,
///     json_map!["foo".to_string() => 42.into(), "bar".to_string() => 1337.into()]
/// );
/// ```
#[macro_export]
macro_rules! json_map {
    (@single $($x:tt)*) => (());
    (@count $($rest:expr),*) => (<[()]>::len(&[$(json_map!(@single $rest)),*]));

    ($($key:expr => $value:expr,)+) => { json_map!($($key => $value),+) };
    ($($key:expr => $value:expr),*) => {
        {
            let _cap = json_map!(@count $($key),*);
            let mut _map = ::serde_json::Map::with_capacity(_cap);
            $(
                _map.insert($key, $value);
            )*
            _map
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
