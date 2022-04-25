use crate::raster::{
    data_type::DefaultNoDataValue, EmptyGrid, Grid, GridOrEmpty, GridOrEmpty2D, GridSize,
    MapPixels, MapPixelsParallel, RasterTile2D,
};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};
use std::ops::{Add, Div, Mul, Sub};

/// scales the value with `(value - offset_by) / scale_with`.
#[inline]
fn scale<T>(value: T, scale_with: T, offset_by: T) -> Option<T>
where
    T: Copy + 'static + Sub<Output = T> + Div<Output = T>,
{
    Some((value - offset_by) / scale_with)
}

/// unscales the value with `value * scale_with + offset_by`.
#[inline]
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
    fn scale(self, scale_with: P, offset_by: P) -> Self::Output;
}

impl<P, G> ScaleElements<P> for Grid<G, P>
where
    P: Scale + Copy + 'static + PartialEq + DefaultNoDataValue,
    G: GridSize + Clone,
{
    type Output = Grid<G, P>;

    fn scale(self, scale_with: P, offset_by: P) -> Self::Output {
        let out_no_data = self
            .no_data_value
            .and_then(|v| v.scale(scale_with, offset_by));
        self.map_pixels(|p| p.scale(scale_with, offset_by), out_no_data)
    }
}

impl<P, G> ScaleElements<P> for GridOrEmpty<G, P>
where
    P: Scale + Copy + 'static + PartialEq + DefaultNoDataValue,
    G: GridSize + Clone,
    Grid<G, P>: ScaleElements<P, Output = Grid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;

    fn scale(self, scale_with: P, offset_by: P) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::Grid(g.scale(scale_with, offset_by)),
            GridOrEmpty::Empty(e) => GridOrEmpty::Empty(EmptyGrid::new(
                e.shape,
                e.no_data_value
                    .scale(scale_with, offset_by)
                    .unwrap_or(P::DEFAULT_NO_DATA_VALUE),
            )),
        }
    }
}

impl<P> ScaleElements<P> for RasterTile2D<P>
where
    P: Scale + 'static + Copy + PartialEq + DefaultNoDataValue,
    GridOrEmpty2D<P>: ScaleElements<P, Output = GridOrEmpty2D<P>>,
{
    type Output = RasterTile2D<P>;

    fn scale(self, scale_with: P, offset_by: P) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.scale(scale_with, offset_by),
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
    fn scale_elements_parallel(self, scale_with: P, offset_by: P) -> Self::Output;
}

impl<P, G> ScaleElementsParallel<P> for Grid<G, P>
where
    P: Scale + Copy + PartialEq + 'static + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
{
    type Output = Grid<G, P>;

    fn scale_elements_parallel(self, scale_with: P, offset_by: P) -> Self::Output {
        let out_no_data = self
            .no_data_value
            .and_then(|v| v.scale(scale_with, offset_by));
        self.map_pixels_parallel(|p| p.scale(scale_with, offset_by), out_no_data)
    }
}

impl<P, G> ScaleElementsParallel<P> for GridOrEmpty<G, P>
where
    P: Scale + Copy + PartialEq + 'static + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
    Grid<G, P>: ScaleElementsParallel<P, Output = Grid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;

    fn scale_elements_parallel(self, scale_with: P, offset_by: P) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => {
                GridOrEmpty::Grid(g.scale_elements_parallel(scale_with, offset_by))
            }
            GridOrEmpty::Empty(e) => GridOrEmpty::Empty(EmptyGrid::new(
                e.shape,
                e.no_data_value
                    .scale(scale_with, offset_by)
                    .unwrap_or(P::DEFAULT_NO_DATA_VALUE),
            )),
        }
    }
}

impl<P> ScaleElementsParallel<P> for RasterTile2D<P>
where
    P: Scale + 'static + Copy + PartialEq + DefaultNoDataValue,
    GridOrEmpty2D<P>: ScaleElementsParallel<P, Output = GridOrEmpty2D<P>>,
{
    type Output = RasterTile2D<P>;

    fn scale_elements_parallel(self, scale_with: P, offset_by: P) -> Self::Output {
        RasterTile2D {
            grid_array: self
                .grid_array
                .scale_elements_parallel(scale_with, offset_by),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
            time: self.time,
        }
    }
}

pub trait UnscaleElements<P> {
    type Output;
    fn unscale_elements(self, scale_with: P, offset_by: P) -> Self::Output;
}

impl<P, G> UnscaleElements<P> for Grid<G, P>
where
    P: Unscale + Copy + PartialEq + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
{
    type Output = Grid<G, P>;
    fn unscale_elements(self, scale_with: P, offset_by: P) -> Self::Output {
        let out_no_data = self
            .no_data_value
            .and_then(|v| v.unscale(scale_with, offset_by));
        self.map_pixels(|p| p.unscale(scale_with, offset_by), out_no_data)
    }
}
impl<P, G> UnscaleElements<P> for GridOrEmpty<G, P>
where
    P: Unscale + Copy + PartialEq + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
    Grid<G, P>: UnscaleElements<P, Output = Grid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;
    fn unscale_elements(self, scale_with: P, offset_by: P) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => GridOrEmpty::Grid(g.unscale_elements(scale_with, offset_by)),
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
    fn unscale_elements(self, scale_with: P, offset_by: P) -> Self::Output {
        RasterTile2D {
            grid_array: self.grid_array.unscale_elements(scale_with, offset_by),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
            time: self.time,
        }
    }
}

pub trait UnscaleElementsParallel<P> {
    type Output;
    fn unscale_elements_parallel(self, scale_with: P, offset_by: P) -> Self::Output;
}

impl<P, G> UnscaleElementsParallel<P> for Grid<G, P>
where
    P: Unscale + Copy + PartialEq + 'static + Send + Sync + DefaultNoDataValue,
    G: GridSize + Clone + Send + Sync,
{
    type Output = Grid<G, P>;
    fn unscale_elements_parallel(self, scale_with: P, offset_by: P) -> Self::Output {
        let out_no_data = self
            .no_data_value
            .and_then(|v| v.unscale(scale_with, offset_by));
        self.map_pixels_parallel(|p| p.unscale(scale_with, offset_by), out_no_data)
    }
}
impl<P, G> UnscaleElementsParallel<P> for GridOrEmpty<G, P>
where
    P: Unscale + Copy + PartialEq + 'static + DefaultNoDataValue,
    G: GridSize + Clone,
    Grid<G, P>: UnscaleElementsParallel<P, Output = Grid<G, P>>,
{
    type Output = GridOrEmpty<G, P>;
    fn unscale_elements_parallel(self, scale_with: P, offset_by: P) -> Self::Output {
        match self {
            GridOrEmpty::Grid(g) => {
                GridOrEmpty::Grid(g.unscale_elements_parallel(scale_with, offset_by))
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
    fn unscale_elements_parallel(self, scale_with: P, offset_by: P) -> Self::Output {
        RasterTile2D {
            grid_array: self
                .grid_array
                .unscale_elements_parallel(scale_with, offset_by),
            global_geo_transform: self.global_geo_transform,
            properties: self.properties,
            tile_position: self.tile_position,
            time: self.time,
        }
    }
}
