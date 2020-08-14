use crate::util::arrow::ArrowTyped;
use arrow::array::Float64Builder;
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::{fmt, slice};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[repr(C)]
pub struct Coordinate2D {
    pub x: f64,
    pub y: f64,
}

impl Coordinate2D {
    /// Creates a new coordinate
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::Coordinate2D;
    ///
    /// let c = Coordinate2D::new(1.0, 0.0);
    ///
    /// assert_eq!(c.x, 1.0);
    /// assert_eq!(c.y, 0.0);
    /// ```
    ///
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

impl fmt::Display for Coordinate2D {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.x, self.y)
    }
}

impl From<(f64, f64)> for Coordinate2D {
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::Coordinate2D;
    ///
    /// let c = Coordinate2D::from((5.0, 4.2));
    ///
    /// assert_eq!(c.x, 5.0);
    /// assert_eq!(c.y, 4.2);
    ///
    /// let c: Coordinate2D = (5.1, -3.0).into();
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

impl From<[f64; 2]> for Coordinate2D {
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::Coordinate2D;
    ///
    /// let c = Coordinate2D::from([5.0, 4.2]);
    ///
    /// assert_eq!(c.x, 5.0);
    /// assert_eq!(c.y, 4.2);
    ///
    /// let c: Coordinate2D = [5.1, -3.0].into();
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

impl Into<(f64, f64)> for Coordinate2D {
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::Coordinate2D;
    ///
    /// let c = Coordinate2D::new(-1.9, 0.04);
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

impl Into<[f64; 2]> for Coordinate2D {
    fn into(self) -> [f64; 2] {
        [self.x, self.y]
    }
}

impl<'c> Into<&'c [f64]> for &'c Coordinate2D {
    fn into(self) -> &'c [f64] {
        unsafe { slice::from_raw_parts(self as *const Coordinate2D as *const f64, 2) }
    }
}

impl ArrowTyped for Coordinate2D {
    type ArrowArray = arrow::array::FixedSizeListArray;
    type ArrowBuilder = arrow::array::FixedSizeListBuilder<Float64Builder>;

    fn arrow_data_type() -> DataType {
        arrow::datatypes::DataType::FixedSizeList(Box::new(arrow::datatypes::DataType::Float64), 2)
    }

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder {
        arrow::array::FixedSizeListBuilder::new(arrow::array::Float64Builder::new(capacity * 2), 2)
    }
}

impl AsRef<[f64]> for Coordinate2D {
    fn as_ref(&self) -> &[f64] {
        let raw_ptr = self as *const Coordinate2D as *const f64;
        unsafe { std::slice::from_raw_parts(raw_ptr, 2) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::mem;

    #[test]
    fn byte_size() {
        assert_eq!(mem::size_of::<Coordinate2D>(), 2 * mem::size_of::<f64>());
        assert_eq!(mem::size_of::<Coordinate2D>(), 2 * 8);
    }
}
