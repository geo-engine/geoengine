use crate::raster::{
    EmptyGrid, GridOrEmpty, GridOrEmpty2D, GridSize, MapElements, MaskedGrid, RasterTile2D,
};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};
use std::ops::{Add, Div, Mul, Sub};

/// scales the value with `(value - offset) / slope`.
#[inline]
#[allow(clippy::unnecessary_wraps)]
fn sub_then_div<T>(value: T, slope: T, offset: T) -> Option<T>
where
    T: Copy + 'static + Sub<Output = T> + Div<Output = T>,
{
    Some((value - offset) / slope)
}

/// unscales the value with `value * slope + offset`.
#[inline]
#[allow(clippy::unnecessary_wraps)]
fn mul_then_add<T>(value: T, slope: T, offset: T) -> Option<T>
where
    T: Copy + 'static + Add<Output = T> + Mul<Output = T>,
{
    Some(value * slope + offset)
}
/// scales the value with `(value - offset) / slope`. Overflows produce `None`.
#[inline]
fn checked_sub_then_div<T>(value: T, slope: T, offset: T) -> Option<T>
where
    T: Copy + 'static + CheckedSub<Output = T> + CheckedDiv<Output = T>,
{
    value
        .checked_sub(&offset)
        .and_then(|f| f.checked_div(&slope))
}

/// unscales the value with ``value * slope + offset``. Overflows produce `None`.
#[inline]
fn checked_mul_then_add<T>(value: T, slope: T, offset: T) -> Option<T>
where
    T: Copy + 'static + CheckedAdd<Output = T> + CheckedMul<Output = T>,
{
    value
        .checked_mul(&slope)
        .and_then(|f| f.checked_add(&offset))
}

pub trait CheckedSubThenDiv
where
    Self: Sized,
{
    /// scales with `(self - offset) / slope`. Overflows produce `None`.
    fn checked_sub_then_div(self, slope: Self, offset: Self) -> Option<Self>;
}

macro_rules! impl_scale_conv {
    ($T:ty, $conv:ident) => {
        impl CheckedSubThenDiv for $T {
            #[inline]
            fn checked_sub_then_div(self, slope: Self, offset: Self) -> Option<Self> {
                $conv(self, slope, offset)
            }
        }
    };
}

impl_scale_conv!(u8, checked_sub_then_div);
impl_scale_conv!(u16, checked_sub_then_div);
impl_scale_conv!(u32, checked_sub_then_div);
impl_scale_conv!(u64, checked_sub_then_div);
impl_scale_conv!(i8, checked_sub_then_div);
impl_scale_conv!(i16, checked_sub_then_div);
impl_scale_conv!(i32, checked_sub_then_div);
impl_scale_conv!(i64, checked_sub_then_div);
impl_scale_conv!(f32, sub_then_div);
impl_scale_conv!(f64, sub_then_div);

pub trait CheckedMulThenAdd {
    /// unscales with `self * slope + offset`. Overflows produce `None`.
    fn checked_mul_then_add(self, slope: Self, offset: Self) -> Option<Self>
    where
        Self: Sized;
}

macro_rules! impl_unscale_conv {
    ($T:ty, $conv:ident) => {
        impl CheckedMulThenAdd for $T {
            #[inline]
            fn checked_mul_then_add(self, slope: Self, offset: Self) -> Option<Self> {
                $conv(self, slope, offset)
            }
        }
    };
}

impl_unscale_conv!(u8, checked_mul_then_add);
impl_unscale_conv!(u16, checked_mul_then_add);
impl_unscale_conv!(u32, checked_mul_then_add);
impl_unscale_conv!(u64, checked_mul_then_add);
impl_unscale_conv!(i8, checked_mul_then_add);
impl_unscale_conv!(i16, checked_mul_then_add);
impl_unscale_conv!(i32, checked_mul_then_add);
impl_unscale_conv!(i64, checked_mul_then_add);
impl_unscale_conv!(f32, mul_then_add);
impl_unscale_conv!(f64, mul_then_add);

pub trait ScalingTransformation<T> {
    fn transform(value: T, slope: T, offset: T) -> Option<T>;
}

/// scales the value with `(value - offset) / slope`. Overflows produce `None`.
pub struct CheckedSubThenDivTransformation;

impl<T> ScalingTransformation<T> for CheckedSubThenDivTransformation
where
    T: CheckedSubThenDiv,
{
    fn transform(value: T, slope: T, offset: T) -> Option<T> {
        value.checked_sub_then_div(slope, offset)
    }
}

/// unscales with `self * slope + offset`. Overflows produce `None`.
pub struct CheckedMulThenAddTransformation;

impl<T> ScalingTransformation<T> for CheckedMulThenAddTransformation
where
    T: CheckedMulThenAdd,
{
    fn transform(value: T, slope: T, offset: T) -> Option<T> {
        value.checked_mul_then_add(slope, offset)
    }
}

/// applies a transformation to an element of a grid
pub trait ElementScaling<T> {
    type Output;
    /// applies a transformation to the elements of the grid.
    fn transform_elements<F: ScalingTransformation<T>>(self, slope: T, offset: T) -> Self::Output;
}

impl<P, G> ElementScaling<P> for MaskedGrid<G, P>
where
    P: Copy + 'static + PartialEq + Default,
    G: GridSize + Clone + PartialEq,
{
    type Output = MaskedGrid<G, P>;

    fn transform_elements<F: ScalingTransformation<P>>(self, slope: P, offset: P) -> Self::Output {
        let map_fn = |vo: Option<P>| vo.and_then(|v| F::transform(v, slope, offset));
        self.map_elements(map_fn)
    }
}

impl<P, G> ElementScaling<P> for GridOrEmpty<G, P>
where
    P: Copy + 'static + PartialEq + Default,
    G: GridSize + Clone + PartialEq,
    MaskedGrid<G, P>: ElementScaling<P, Output = MaskedGrid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;

    fn transform_elements<F: ScalingTransformation<P>>(self, slope: P, offset: P) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::Grid(g.transform_elements::<F>(slope, offset)),
            GridOrEmpty::Empty(e) => GridOrEmpty::Empty(EmptyGrid::new(e.shape)),
        }
    }
}

impl<P> ElementScaling<P> for RasterTile2D<P>
where
    P: 'static + Copy + PartialEq,
    GridOrEmpty2D<P>: ElementScaling<P, Output = GridOrEmpty2D<P>>,
{
    type Output = RasterTile2D<P>;

    fn transform_elements<F: ScalingTransformation<P>>(self, slope: P, offset: P) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.transform_elements::<F>(slope, offset),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
            time: self.time,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        primitives::TimeInterval,
        raster::{GeoTransform, Grid2D, MaskedGrid2D},
        util::test::TestDefault,
    };

    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn unscale_float() {
        let unscaled = mul_then_add(7., 2., 1.).unwrap();
        assert_eq!(unscaled, 15.);
    }

    #[test]
    fn unscale_checked_int() {
        let unscaled = checked_mul_then_add(7, 2, 1).unwrap();
        assert_eq!(unscaled, 15);

        let unscaled: Option<u8> = checked_mul_then_add(7, 100, 1);
        assert!(unscaled.is_none());

        let unscaled: Option<u8> = checked_mul_then_add(7, 2, 255);
        assert!(unscaled.is_none());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn scale_float() {
        let scaled = sub_then_div(15., 2., 1.).unwrap();
        assert_eq!(scaled, 7.);
    }

    #[test]
    fn scale_checked_int() {
        let scaled = checked_sub_then_div(15, 2, 1).unwrap();
        assert_eq!(scaled, 7);

        let scaled: Option<u8> = checked_sub_then_div(7, 1, 10);
        assert!(scaled.is_none());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn unscale_float_self() {
        let unscaled = (7.).checked_mul_then_add(2., 1.).unwrap();
        assert_eq!(unscaled, 15.);
    }

    #[test]
    fn unscale_checked_int_self() {
        let unscaled = 7.checked_mul_then_add(2, 1).unwrap();
        assert_eq!(unscaled, 15);

        let unscaled: Option<u8> = 7.checked_mul_then_add(100, 1);
        assert!(unscaled.is_none());

        let unscaled: Option<u8> = 7.checked_mul_then_add(2, 255);
        assert!(unscaled.is_none());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn scale_float_self() {
        let scaled = (15.).checked_sub_then_div(2., 1.).unwrap();
        assert_eq!(scaled, 7.);
    }

    #[test]
    fn scale_checked_int_self() {
        let scaled = 15.checked_sub_then_div(2, 1).unwrap();
        assert_eq!(scaled, 7);

        let scaled: Option<u8> = 7.checked_sub_then_div(1, 10);
        assert!(scaled.is_none());
    }

    #[test]
    fn unscale_grid() {
        let dim = [2, 2];
        let data = vec![7; 4];

        let r1: MaskedGrid2D<i32> = Grid2D::new(dim.into(), data).unwrap().into();
        let scaled_r1 = r1.transform_elements::<CheckedMulThenAddTransformation>(2, 1);

        let expected = vec![Some(15), Some(15), Some(15), Some(15)];
        let res: Vec<Option<i32>> = scaled_r1.masked_element_deref_iterator().collect();
        assert_eq!(expected, res);
    }

    #[test]
    fn unscale_grid_or_empty() {
        let dim = [2, 2];
        let data = vec![7; 4];

        let r1: GridOrEmpty2D<i32> =
            GridOrEmpty::Grid(Grid2D::new(dim.into(), data).unwrap().into());
        let scaled_r1 = r1.transform_elements::<CheckedMulThenAddTransformation>(2, 1);

        let expected = vec![Some(15), Some(15), Some(15), Some(15)];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                let res: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(expected, res);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn unscale_raster_tile() {
        let dim = [2, 2];
        let data = vec![7; 4];
        let geo = GeoTransform::test_default();

        let r1: GridOrEmpty2D<i32> =
            GridOrEmpty::Grid(Grid2D::new(dim.into(), data).unwrap().into());
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.transform_elements::<CheckedMulThenAddTransformation>(2, 1);
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = vec![Some(15), Some(15), Some(15), Some(15)];
        let res: Vec<Option<i32>> = mat_scaled_r1
            .grid_array
            .masked_element_deref_iterator()
            .collect();
        assert_eq!(expected, res);
    }

    #[test]
    fn scale_grid() {
        let dim = [2, 2];
        let data = vec![15; 4];

        let r1: MaskedGrid2D<i32> = Grid2D::new(dim.into(), data).unwrap().into();
        let scaled_r1 = r1.transform_elements::<CheckedSubThenDivTransformation>(2, 1);

        let expected = vec![Some(7), Some(7), Some(7), Some(7)];
        let res: Vec<Option<i32>> = scaled_r1.masked_element_deref_iterator().collect();
        assert_eq!(expected, res);
    }

    #[test]
    fn scale_grid_or_empty() {
        let dim = [2, 2];
        let data = vec![15; 4];
        let r1: GridOrEmpty2D<i32> =
            GridOrEmpty::Grid(Grid2D::new(dim.into(), data).unwrap().into());
        let scaled_r1 = r1.transform_elements::<CheckedSubThenDivTransformation>(2, 1);

        let expected = vec![Some(7), Some(7), Some(7), Some(7)];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                let res: Vec<Option<i32>> = g.masked_element_deref_iterator().collect();
                assert_eq!(expected, res);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn scale_raster_tile() {
        let dim = [2, 2];
        let data = vec![15; 4];
        let geo = GeoTransform::test_default();

        let r1: GridOrEmpty2D<i32> = GridOrEmpty2D::from(Grid2D::new(dim.into(), data).unwrap());
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.transform_elements::<CheckedSubThenDivTransformation>(2, 1);
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = vec![Some(7), Some(7), Some(7), Some(7)];
        let res: Vec<Option<i32>> = mat_scaled_r1
            .grid_array
            .masked_element_deref_iterator()
            .collect();
        assert_eq!(expected, res);
    }
}
