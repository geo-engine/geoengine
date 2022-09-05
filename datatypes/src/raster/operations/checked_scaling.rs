use crate::raster::{
    EmptyGrid, GridOrEmpty, GridOrEmpty2D, GridSize, MapElements, MaskedGrid, RasterTile2D,
};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};
use std::ops::{Add, Div, Mul, Sub};

/// scales the value with `(value - offset_by) / scale_with`.
#[inline]
#[allow(clippy::unnecessary_wraps)]
fn scale<T>(value: T, scale_with: T, offset_by: T) -> Option<T>
where
    T: Copy + 'static + Sub<Output = T> + Div<Output = T>,
{
    Some((value - offset_by) / scale_with)
}

/// unscales the value with `value * scale_with + offset_by`.
#[inline]
#[allow(clippy::unnecessary_wraps)]
fn unscale<T>(value: T, scale_with: T, offset_by: T) -> Option<T>
where
    T: Copy + 'static + Add<Output = T> + Mul<Output = T>,
{
    Some(value * scale_with + offset_by)
}
/// scales the value with `(value - offset_by) / scale_with`. Overflows produce `None`.
#[inline]
fn scale_checked<T>(value: T, scale_with: T, offset_by: T) -> Option<T>
where
    T: Copy + 'static + CheckedSub<Output = T> + CheckedDiv<Output = T>,
{
    value
        .checked_sub(&offset_by)
        .and_then(|f| f.checked_div(&scale_with))
}

/// unscales the value with ``value * scale_with + offset_by``. Overflows produce `None`.
#[inline]
fn unscale_checked<T>(value: T, scale_with: T, offset_by: T) -> Option<T>
where
    T: Copy + 'static + CheckedAdd<Output = T> + CheckedMul<Output = T>,
{
    value
        .checked_mul(&scale_with)
        .and_then(|f| f.checked_add(&offset_by))
}

pub trait Scale
where
    Self: Sized,
{
    /// scales with `(self - offset_by) / scale_with`. Overflows produce `None`.
    fn scale(self, scale_with: Self, offset_by: Self) -> Option<Self>;
}

macro_rules! impl_scale_conv {
    ($T:ty, $conv:ident) => {
        impl Scale for $T {
            #[inline]
            fn scale(self, scale_with: Self, offset_by: Self) -> Option<Self> {
                $conv(self, scale_with, offset_by)
            }
        }
    };
}

impl_scale_conv!(u8, scale_checked);
impl_scale_conv!(u16, scale_checked);
impl_scale_conv!(u32, scale_checked);
impl_scale_conv!(u64, scale_checked);
impl_scale_conv!(i8, scale_checked);
impl_scale_conv!(i16, scale_checked);
impl_scale_conv!(i32, scale_checked);
impl_scale_conv!(i64, scale_checked);
impl_scale_conv!(f32, scale);
impl_scale_conv!(f64, scale);

pub trait Unscale {
    /// unscales with `self * scale_with + offset_by`. Overflows produce `None`.
    fn unscale(self, scale_with: Self, offset_by: Self) -> Option<Self>
    where
        Self: Sized;
}

macro_rules! impl_unscale_conv {
    ($T:ty, $conv:ident) => {
        impl Unscale for $T {
            #[inline]
            fn unscale(self, scale_with: Self, offset_by: Self) -> Option<Self> {
                $conv(self, scale_with, offset_by)
            }
        }
    };
}

impl_unscale_conv!(u8, unscale_checked);
impl_unscale_conv!(u16, unscale_checked);
impl_unscale_conv!(u32, unscale_checked);
impl_unscale_conv!(u64, unscale_checked);
impl_unscale_conv!(i8, unscale_checked);
impl_unscale_conv!(i16, unscale_checked);
impl_unscale_conv!(i32, unscale_checked);
impl_unscale_conv!(i64, unscale_checked);
impl_unscale_conv!(f32, unscale);
impl_unscale_conv!(f64, unscale);

pub trait ScalingTransformation<T> {
    fn transform(value: T, scale_with: T, offset_by: T) -> Option<T>;
}

/// scales the value with `(value - offset_by) / scale_with`. Overflows produce `None`.
pub struct ScaleTransformation;

impl<T> ScalingTransformation<T> for ScaleTransformation
where
    T: Scale,
{
    fn transform(value: T, scale_with: T, offset_by: T) -> Option<T> {
        value.scale(scale_with, offset_by)
    }
}

/// unscales with `self * scale_with + offset_by`. Overflows produce `None`.
pub struct UnscaleTransformation;

impl<T> ScalingTransformation<T> for UnscaleTransformation
where
    T: Unscale,
{
    fn transform(value: T, scale_with: T, offset_by: T) -> Option<T> {
        value.unscale(scale_with, offset_by)
    }
}

/// applies a transformation to an element of a grid
pub trait ElementScaling<T> {
    type Output;
    /// applies a transformation to the elements of the grid.
    fn transform_elements<F: ScalingTransformation<T>>(
        self,
        scale_with: T,
        offset_by: T,
    ) -> Self::Output;
}

impl<P, G> ElementScaling<P> for MaskedGrid<G, P>
where
    P: Copy + 'static + PartialEq + Default,
    G: GridSize + Clone + PartialEq,
{
    type Output = MaskedGrid<G, P>;

    fn transform_elements<F: ScalingTransformation<P>>(
        self,
        scale_with: P,
        offset_by: P,
    ) -> Self::Output {
        let map_fn = |vo: Option<P>| vo.and_then(|v| F::transform(v, scale_with, offset_by));
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

    fn transform_elements<F: ScalingTransformation<P>>(
        self,
        scale_with: P,
        offset_by: P,
    ) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => {
                GridOrEmpty::Grid(g.transform_elements::<F>(scale_with, offset_by))
            }
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

    fn transform_elements<F: ScalingTransformation<P>>(
        self,
        scale_with: P,
        offset_by: P,
    ) -> Self::Output {
        RasterTile2D {
            grid_array: self
                .grid_array
                .transform_elements::<F>(scale_with, offset_by),
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
        let unscaled = unscale(7., 2., 1.).unwrap();
        assert_eq!(unscaled, 15.);
    }

    #[test]
    fn unscale_checked_int() {
        let unscaled = unscale_checked(7, 2, 1).unwrap();
        assert_eq!(unscaled, 15);

        let unscaled: Option<u8> = unscale_checked(7, 100, 1);
        assert!(unscaled.is_none());

        let unscaled: Option<u8> = unscale_checked(7, 2, 255);
        assert!(unscaled.is_none());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn scale_float() {
        let scaled = scale(15., 2., 1.).unwrap();
        assert_eq!(scaled, 7.);
    }

    #[test]
    fn scale_checked_int() {
        let scaled = scale_checked(15, 2, 1).unwrap();
        assert_eq!(scaled, 7);

        let scaled: Option<u8> = scale_checked(7, 1, 10);
        assert!(scaled.is_none());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn unscale_float_self() {
        let unscaled = (7.).unscale(2., 1.).unwrap();
        assert_eq!(unscaled, 15.);
    }

    #[test]
    fn unscale_checked_int_self() {
        let unscaled = 7.unscale(2, 1).unwrap();
        assert_eq!(unscaled, 15);

        let unscaled: Option<u8> = 7.unscale(100, 1);
        assert!(unscaled.is_none());

        let unscaled: Option<u8> = 7.unscale(2, 255);
        assert!(unscaled.is_none());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn scale_float_self() {
        let scaled = (15.).scale(2., 1.).unwrap();
        assert_eq!(scaled, 7.);
    }

    #[test]
    fn scale_checked_int_self() {
        let scaled = 15.scale(2, 1).unwrap();
        assert_eq!(scaled, 7);

        let scaled: Option<u8> = 7.scale(1, 10);
        assert!(scaled.is_none());
    }

    #[test]
    fn unscale_grid() {
        let dim = [2, 2];
        let data = vec![7; 4];

        let r1: MaskedGrid2D<i32> = Grid2D::new(dim.into(), data).unwrap().into();
        let scaled_r1 = r1.transform_elements::<UnscaleTransformation>(2, 1);

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
        let scaled_r1 = r1.transform_elements::<UnscaleTransformation>(2, 1);

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

        let scaled_r1 = t1.transform_elements::<UnscaleTransformation>(2, 1);
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
        let scaled_r1 = r1.transform_elements::<ScaleTransformation>(2, 1);

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
        let scaled_r1 = r1.transform_elements::<ScaleTransformation>(2, 1);

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

        let scaled_r1 = t1.transform_elements::<ScaleTransformation>(2, 1);
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = vec![Some(7), Some(7), Some(7), Some(7)];
        let res: Vec<Option<i32>> = mat_scaled_r1
            .grid_array
            .masked_element_deref_iterator()
            .collect();
        assert_eq!(expected, res);
    }
}
