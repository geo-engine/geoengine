use serde::{Deserialize, Serialize};
use std::slice;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[repr(C)]
pub struct Coordinate {
    pub x: f64,
    pub y: f64,
}

impl Coordinate {
    /// Creates a new coordinate
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::Coordinate;
    ///
    /// let c = Coordinate::new(1.0, 0.0);
    ///
    /// assert_eq!(c.x, 1.0);
    /// assert_eq!(c.y, 0.0);
    /// ```
    ///
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

impl From<(f64, f64)> for Coordinate {
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::Coordinate;
    ///
    /// let c = Coordinate::from((5.0, 4.2));
    ///
    /// assert_eq!(c.x, 5.0);
    /// assert_eq!(c.y, 4.2);
    ///
    /// let c: Coordinate = (5.1, -3.0).into();
    ///
    /// assert_eq!(c.x, 5.1);
    /// assert_eq!(c.y, -3.0);
    /// ```
    ///
    fn from(tuple: (f64, f64)) -> Self {
        let (x, y) = tuple;
        Self { x, y }
    }
}

impl From<[f64; 2]> for Coordinate {
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::Coordinate;
    ///
    /// let c = Coordinate::from([5.0, 4.2]);
    ///
    /// assert_eq!(c.x, 5.0);
    /// assert_eq!(c.y, 4.2);
    ///
    /// let c: Coordinate = [5.1, -3.0].into();
    ///
    /// assert_eq!(c.x, 5.1);
    /// assert_eq!(c.y, -3.0);
    /// ```
    ///
    fn from(array: [f64; 2]) -> Self {
        let [x, y] = array;
        Self { x, y }
    }
}

impl Into<(f64, f64)> for Coordinate {
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::Coordinate;
    ///
    /// let c = Coordinate::new(-1.9, 0.04);
    ///
    /// let (x, y) = c.into();
    ///
    /// assert_eq!(x, -1.9);
    /// assert_eq!(y, 0.04);
    /// ```
    ///
    fn into(self) -> (f64, f64) {
        (self.x, self.y)
    }
}

impl Into<[f64; 2]> for Coordinate {
    fn into(self) -> [f64; 2] {
        [self.x, self.y]
    }
}

impl<'c> Into<&'c [f64]> for &'c Coordinate {
    fn into(self) -> &'c [f64] {
        unsafe { slice::from_raw_parts(self as *const Coordinate as *const f64, 2) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::mem;

    #[test]
    fn byte_size() {
        assert_eq!(mem::size_of::<Coordinate>(), 2 * mem::size_of::<f64>());
        assert_eq!(mem::size_of::<Coordinate>(), 2 * 8);
    }
}
