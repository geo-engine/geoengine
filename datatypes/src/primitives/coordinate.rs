use crate::util::arrow::ArrowTyped;
use arrow::array::{ArrayBuilder, BooleanArray, Float64Builder};
use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;
use float_cmp::ApproxEq;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, ToSql};
use proj::Coord;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    ops::{Add, Div, Mul, Sub},
    slice,
};
use utoipa::Component;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, PartialOrd, Serialize, Default, Component)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
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

    #[must_use]
    pub fn min_elements(&self, other: Self) -> Self {
        Coordinate2D {
            x: self.x.min(other.x),
            y: self.y.min(other.y),
        }
    }

    #[must_use]
    pub fn max_elements(&self, other: Self) -> Self {
        Coordinate2D {
            x: self.x.max(other.x),
            y: self.y.max(other.y),
        }
    }

    pub fn euclidean_distance(&self, other: &Self) -> f64 {
        let x_diff = self.x - other.x;
        let y_diff = self.y - other.y;
        let sq_sum = x_diff * x_diff + y_diff * y_diff;
        sq_sum.sqrt()
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

impl From<Coordinate2D> for (f64, f64) {
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
    fn from(coordinate: Coordinate2D) -> (f64, f64) {
        (coordinate.x, coordinate.y)
    }
}

impl From<Coordinate2D> for [f64; 2] {
    fn from(coordinate: Coordinate2D) -> [f64; 2] {
        [coordinate.x, coordinate.y]
    }
}

impl<'c> From<&'c Coordinate2D> for &'c [f64] {
    fn from(coordinate: &'c Coordinate2D) -> &'c [f64] {
        unsafe { slice::from_raw_parts((coordinate as *const Coordinate2D).cast::<f64>(), 2) }
    }
}

impl From<Coordinate2D> for geo::Coordinate<f64> {
    fn from(coordinate: Coordinate2D) -> geo::Coordinate<f64> {
        Self::from(&coordinate)
    }
}

impl From<&Coordinate2D> for geo::Coordinate<f64> {
    fn from(coordinate: &Coordinate2D) -> geo::Coordinate<f64> {
        geo::Coordinate::from((coordinate.x, coordinate.y))
    }
}

impl From<geo::Coordinate<f64>> for Coordinate2D {
    fn from(coordinate: geo::Coordinate<f64>) -> Coordinate2D {
        Coordinate2D {
            x: coordinate.x,
            y: coordinate.y,
        }
    }
}

impl From<geo::Point<f64>> for Coordinate2D {
    fn from(point: geo::Point<f64>) -> Coordinate2D {
        Coordinate2D {
            x: point.0.x,
            y: point.0.y,
        }
    }
}

impl Coord<f64> for Coordinate2D {
    fn x(&self) -> f64 {
        self.x
    }

    fn y(&self) -> f64 {
        self.y
    }

    fn from_xy(x: f64, y: f64) -> Self {
        Coordinate2D::new(x, y)
    }
}

impl ArrowTyped for Coordinate2D {
    type ArrowArray = arrow::array::FixedSizeListArray;
    type ArrowBuilder = arrow::array::FixedSizeListBuilder<Float64Builder>;

    fn arrow_data_type() -> DataType {
        let nullable = true; // TODO: should actually be false, but arrow's builders set it to `true` currently

        DataType::FixedSizeList(Box::new(Field::new("item", DataType::Float64, nullable)), 2)
    }

    fn builder_byte_size(builder: &mut Self::ArrowBuilder) -> usize {
        builder.values().len() * std::mem::size_of::<f64>()
    }

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder {
        arrow::array::FixedSizeListBuilder::new(arrow::array::Float64Builder::new(capacity * 2), 2)
    }

    fn concat(
        _a: &Self::ArrowArray,
        _b: &Self::ArrowArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        unimplemented!("This is not used by now")
    }

    fn filter(
        _data_array: &Self::ArrowArray,
        _filter_array: &BooleanArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        unimplemented!("This is not used by now")
    }

    fn from_vec(_data: Vec<Self>) -> Result<Self::ArrowArray, ArrowError>
    where
        Self: Sized,
    {
        unimplemented!("This is not used by now")
    }
}

impl AsRef<[f64]> for Coordinate2D {
    fn as_ref(&self) -> &[f64] {
        let raw_ptr = (self as *const Coordinate2D).cast::<f64>();
        unsafe { std::slice::from_raw_parts(raw_ptr, 2) }
    }
}

impl Add for Coordinate2D {
    type Output = Coordinate2D;

    fn add(self, rhs: Self) -> Self::Output {
        Coordinate2D::new(self.x + rhs.x, self.y + rhs.y)
    }
}

impl Add<f64> for Coordinate2D {
    type Output = Coordinate2D;

    fn add(self, rhs: f64) -> Self::Output {
        Coordinate2D::new(self.x + rhs, self.y + rhs)
    }
}

impl Sub for Coordinate2D {
    type Output = Coordinate2D;

    fn sub(self, rhs: Self) -> Self::Output {
        Coordinate2D::new(self.x - rhs.x, self.y - rhs.y)
    }
}

impl Sub<f64> for Coordinate2D {
    type Output = Coordinate2D;

    fn sub(self, rhs: f64) -> Self::Output {
        Coordinate2D::new(self.x - rhs, self.y - rhs)
    }
}

impl Mul for Coordinate2D {
    type Output = Coordinate2D;

    fn mul(self, rhs: Self) -> Self::Output {
        Coordinate2D::new(self.x * rhs.x, self.y * rhs.y)
    }
}

impl Mul<f64> for Coordinate2D {
    type Output = Coordinate2D;

    fn mul(self, rhs: f64) -> Self::Output {
        Coordinate2D::new(self.x * rhs, self.y * rhs)
    }
}

impl Div for Coordinate2D {
    type Output = Coordinate2D;

    fn div(self, rhs: Self) -> Self::Output {
        Coordinate2D::new(self.x / rhs.x, self.y / rhs.y)
    }
}

impl Div<f64> for Coordinate2D {
    type Output = Coordinate2D;

    fn div(self, rhs: f64) -> Self::Output {
        Coordinate2D::new(self.x / rhs, self.y / rhs)
    }
}

impl ApproxEq for Coordinate2D {
    type Margin = float_cmp::F64Margin;

    fn approx_eq<M>(self, other: Self, margin: M) -> bool
    where
        M: Into<Self::Margin>,
    {
        let m = margin.into();
        self.x.approx_eq(other.x, m) && self.y.approx_eq(other.y, m)
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

    #[test]
    fn add() {
        let res = Coordinate2D { x: 4., y: 9. } + Coordinate2D { x: 1., y: 1. };
        assert_eq!(res, Coordinate2D { x: 5., y: 10. });
    }

    #[test]
    fn add_scalar() {
        let res = Coordinate2D { x: 4., y: 9. } + 1.;
        assert_eq!(res, Coordinate2D { x: 5., y: 10. });
    }

    #[test]
    fn sub() {
        let res = Coordinate2D { x: 4., y: 9. } - Coordinate2D { x: 1., y: 1. };
        assert_eq!(res, Coordinate2D { x: 3., y: 8. });
    }

    #[test]
    fn sub_scalar() {
        let res = Coordinate2D { x: 4., y: 9. } - 1.;
        assert_eq!(res, Coordinate2D { x: 3., y: 8. });
    }

    #[test]
    fn mul() {
        let res = Coordinate2D { x: 4., y: 9. } * Coordinate2D { x: 2., y: 2. };
        assert_eq!(res, Coordinate2D { x: 8., y: 18. });
    }

    #[test]
    fn mul_scalar() {
        let res = Coordinate2D { x: 4., y: 9. } * 2.;
        assert_eq!(res, Coordinate2D { x: 8., y: 18. });
    }

    #[test]
    fn div() {
        let res = Coordinate2D { x: 4., y: 8. } / Coordinate2D { x: 2., y: 2. };
        assert_eq!(res, Coordinate2D { x: 2., y: 4. });
    }

    #[test]
    fn div_scalar() {
        let res = Coordinate2D { x: 4., y: 8. } / 2.;
        assert_eq!(res, Coordinate2D { x: 2., y: 4. });
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_euclidean_distance() {
        assert_eq!(
            Coordinate2D::new(0., 0.).euclidean_distance(&(0., 1.).into()),
            1.0
        );
        assert_eq!(
            Coordinate2D::new(0., 0.).euclidean_distance(&(1., 0.).into()),
            1.0
        );
        assert_eq!(
            Coordinate2D::new(0., 0.).euclidean_distance(&(1., 1.).into()),
            2.0_f64.sqrt()
        );
    }
}
