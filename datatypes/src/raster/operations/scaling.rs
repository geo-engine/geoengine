use crate::raster::{
    data_type::DefaultNoDataValue, EmptyGrid, Grid, GridOrEmpty, GridOrEmpty2D, GridSize,
    MapElements, MapElementsParallel, RasterTile2D,
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

pub trait ScaleElements<P> {
    type Output;
    /// casts the elements of the collection to the output type and then scales them with `(self - offset_by) / scale_with`. Overflows produce `None`.
    fn scale_elements(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output;
}

impl<P, G> ScaleElements<P> for Grid<G, P>
where
    P: Scale + Copy + 'static + PartialEq + DefaultNoDataValue,
    G: GridSize + Clone,
{
    type Output = Grid<G, P>;

    fn scale_elements(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        self.map_elements(|p| p.scale(scale_with, offset_by), Some(out_no_data))
    }
}

impl<P, G> ScaleElements<P> for GridOrEmpty<G, P>
where
    P: Scale + Copy + 'static + PartialEq + DefaultNoDataValue,
    G: GridSize + Clone,
    Grid<G, P>: ScaleElements<P, Output = Grid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;

    fn scale_elements(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => {
                GridOrEmpty::Grid(g.scale_elements(scale_with, offset_by, out_no_data))
            }
            GridOrEmpty::Empty(e) => GridOrEmpty::Empty(EmptyGrid::new(e.shape, out_no_data)),
        }
    }
}

impl<P> ScaleElements<P> for RasterTile2D<P>
where
    P: Scale + 'static + Copy + PartialEq + DefaultNoDataValue,
    GridOrEmpty2D<P>: ScaleElements<P, Output = GridOrEmpty2D<P>>,
{
    type Output = RasterTile2D<P>;

    fn scale_elements(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        RasterTile2D {
            grid_array: self
                .grid_array
                .scale_elements(scale_with, offset_by, out_no_data),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
            time: self.time,
        }
    }
}

pub trait ScaleElementsParallel<P> {
    type Output;

    /// scales the values of the collection (parallel) with `(self - offset_by) / scale_with`. Overflows produce `None`.
    fn scale_elements_parallel(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output;
}

impl<P, G> ScaleElementsParallel<P> for Grid<G, P>
where
    P: Scale + Copy + PartialEq + 'static + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
{
    type Output = Grid<G, P>;

    fn scale_elements_parallel(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        self.map_elements_parallel(|p| p.scale(scale_with, offset_by), Some(out_no_data))
    }
}

impl<P, G> ScaleElementsParallel<P> for GridOrEmpty<G, P>
where
    P: Scale + Copy + PartialEq + 'static + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
    Grid<G, P>: ScaleElementsParallel<P, Output = Grid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;

    fn scale_elements_parallel(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => {
                GridOrEmpty::Grid(g.scale_elements_parallel(scale_with, offset_by, out_no_data))
            }
            GridOrEmpty::Empty(e) => GridOrEmpty::Empty(EmptyGrid::new(e.shape, out_no_data)),
        }
    }
}

impl<P> ScaleElementsParallel<P> for RasterTile2D<P>
where
    P: Scale + 'static + Copy + PartialEq + DefaultNoDataValue,
    GridOrEmpty2D<P>: ScaleElementsParallel<P, Output = GridOrEmpty2D<P>>,
{
    type Output = RasterTile2D<P>;

    fn scale_elements_parallel(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        RasterTile2D {
            grid_array: self
                .grid_array
                .scale_elements_parallel(scale_with, offset_by, out_no_data),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
            time: self.time,
        }
    }
}

pub trait UnscaleElements<P> {
    type Output;
    fn unscale_elements(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output;
}

impl<P, G> UnscaleElements<P> for Grid<G, P>
where
    P: Unscale + Copy + PartialEq + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
{
    type Output = Grid<G, P>;
    fn unscale_elements(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        self.map_elements(|p| p.unscale(scale_with, offset_by), Some(out_no_data))
    }
}
impl<P, G> UnscaleElements<P> for GridOrEmpty<G, P>
where
    P: Unscale + Copy + PartialEq + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
    Grid<G, P>: UnscaleElements<P, Output = Grid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;
    fn unscale_elements(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => {
                GridOrEmpty::Grid(g.unscale_elements(scale_with, offset_by, out_no_data))
            }
            GridOrEmpty::Empty(e) => GridOrEmpty::Empty(EmptyGrid::new(
                e.shape,
                e.no_data_value
                    .unscale(scale_with, offset_by)
                    .unwrap_or(P::DEFAULT_NO_DATA_VALUE),
            )),
        }
    }
}

impl<P> UnscaleElements<P> for RasterTile2D<P>
where
    P: Unscale + Copy + PartialEq + 'static,
    GridOrEmpty2D<P>: UnscaleElements<P, Output = GridOrEmpty2D<P>>,
{
    type Output = RasterTile2D<P>;
    fn unscale_elements(self, scale_with: P, offset_by: P, out_no_data: P) -> Self::Output {
        RasterTile2D {
            grid_array: self
                .grid_array
                .unscale_elements(scale_with, offset_by, out_no_data),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
            time: self.time,
        }
    }
}

pub trait UnscaleElementsParallel<P> {
    type Output;
    fn unscale_elements_parallel(self, scale_with: P, offset_by: P, out_no_data: P)
        -> Self::Output;
}

impl<P, G> UnscaleElementsParallel<P> for Grid<G, P>
where
    P: Unscale + Copy + PartialEq + 'static + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
{
    type Output = Grid<G, P>;
    fn unscale_elements_parallel(
        self,
        scale_with: P,
        offset_by: P,
        out_no_data: P,
    ) -> Self::Output {
        self.map_elements_parallel(|p| p.unscale(scale_with, offset_by), Some(out_no_data))
    }
}
impl<P, G> UnscaleElementsParallel<P> for GridOrEmpty<G, P>
where
    P: Unscale + Copy + PartialEq + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
    Grid<G, P>: UnscaleElementsParallel<P, Output = Grid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;
    fn unscale_elements_parallel(
        self,
        scale_with: P,
        offset_by: P,
        out_no_data: P,
    ) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => {
                GridOrEmpty::Grid(g.unscale_elements_parallel(scale_with, offset_by, out_no_data))
            }
            GridOrEmpty::Empty(e) => GridOrEmpty::Empty(EmptyGrid::new(
                e.shape,
                e.no_data_value
                    .unscale(scale_with, offset_by)
                    .unwrap_or(P::DEFAULT_NO_DATA_VALUE),
            )),
        }
    }
}

impl<P> UnscaleElementsParallel<P> for RasterTile2D<P>
where
    P: Unscale + Copy + PartialEq + 'static,
    GridOrEmpty2D<P>: UnscaleElementsParallel<P, Output = GridOrEmpty2D<P>>,
{
    type Output = RasterTile2D<P>;
    fn unscale_elements_parallel(
        self,
        scale_with: P,
        offset_by: P,
        out_no_data: P,
    ) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.unscale_elements_parallel(
                scale_with,
                offset_by,
                out_no_data,
            ),
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
        raster::{GeoTransform, Grid2D},
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
        let no_data = 255;

        let r1 = Grid2D::new(dim.into(), data, Some(no_data)).unwrap();
        let scaled_r1 = r1.unscale_elements(2, 1, no_data);

        let expected = [15, 15, 15, 15];
        assert_eq!(scaled_r1.data, expected);
    }

    #[test]
    fn unscale_grid_or_empty() {
        let dim = [2, 2];
        let data = vec![7; 4];
        let no_data = 255;

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let scaled_r1 = r1.unscale_elements(2, 1, no_data);

        let expected = [15, 15, 15, 15];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                assert_eq!(g.data, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn unscale_raster_tile() {
        let dim = [2, 2];
        let data = vec![7; 4];
        let no_data = 255;
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.unscale_elements(2, 1, no_data);
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [15, 15, 15, 15];

        assert_eq!(mat_scaled_r1.grid_array.data, expected);
    }

    #[test]
    fn scale_grid() {
        let dim = [2, 2];
        let data = vec![15; 4];
        let no_data = 255;

        let r1 = Grid2D::new(dim.into(), data, Some(no_data)).unwrap();
        let scaled_r1 = r1.scale_elements(2, 1, no_data);

        let expected = [7, 7, 7, 7];
        assert_eq!(scaled_r1.data, expected);
    }

    #[test]
    fn scale_grid_or_empty() {
        let dim = [2, 2];
        let data = vec![15; 4];
        let no_data = 255;

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let scaled_r1 = r1.scale_elements(2, 1, no_data);

        let expected = [7, 7, 7, 7];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                assert_eq!(g.data, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn scale_raster_tile() {
        let dim = [2, 2];
        let data = vec![15; 4];
        let no_data = 255;
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.scale_elements(2, 1, no_data);
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [7, 7, 7, 7];

        assert_eq!(mat_scaled_r1.grid_array.data, expected);
    }

    #[test]
    fn unscale_grid_parallel() {
        let dim = [2, 2];
        let data = vec![7; 4];
        let no_data = 255;

        let r1 = Grid2D::new(dim.into(), data, Some(no_data)).unwrap();
        let scaled_r1 = r1.unscale_elements_parallel(2, 1, no_data);

        let expected = [15, 15, 15, 15];
        assert_eq!(scaled_r1.data, expected);
    }

    #[test]
    fn unscale_grid_or_empty_parallel() {
        let dim = [2, 2];
        let data = vec![7; 4];
        let no_data = 255;

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let scaled_r1 = r1.unscale_elements_parallel(2, 1, no_data);

        let expected = [15, 15, 15, 15];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                assert_eq!(g.data, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn unscale_raster_tile_parallel() {
        let dim = [2, 2];
        let data = vec![7; 4];
        let no_data = 255;
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.unscale_elements_parallel(2, 1, no_data);
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [15, 15, 15, 15];

        assert_eq!(mat_scaled_r1.grid_array.data, expected);
    }

    #[test]
    fn scale_grid_parallel() {
        let dim = [2, 2];
        let data = vec![15; 4];
        let no_data = 255;

        let r1 = Grid2D::new(dim.into(), data, Some(no_data)).unwrap();
        let scaled_r1 = r1.scale_elements_parallel(2, 1, no_data);

        let expected = [7, 7, 7, 7];
        assert_eq!(scaled_r1.data, expected);
    }

    #[test]
    fn scale_grid_or_empty_parallel() {
        let dim = [2, 2];
        let data = vec![15; 4];
        let no_data = 255;

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let scaled_r1 = r1.scale_elements_parallel(2, 1, no_data);

        let expected = [7, 7, 7, 7];

        match scaled_r1 {
            GridOrEmpty::Grid(g) => {
                assert_eq!(g.data, expected);
            }
            GridOrEmpty::Empty(_) => panic!("Expected grid"),
        }
    }

    #[test]
    fn scale_raster_tile_parallel() {
        let dim = [2, 2];
        let data = vec![15; 4];
        let no_data = 255;
        let geo = GeoTransform::test_default();

        let r1 = GridOrEmpty::Grid(Grid2D::new(dim.into(), data, Some(no_data)).unwrap());
        let t1 = RasterTile2D::new(TimeInterval::default(), [0, 0].into(), geo, r1);

        let scaled_r1 = t1.scale_elements_parallel(2, 1, no_data);
        let mat_scaled_r1 = scaled_r1.into_materialized_tile();

        let expected = [7, 7, 7, 7];

        assert_eq!(mat_scaled_r1.grid_array.data, expected);
    }
}
