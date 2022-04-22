use crate::raster::{Grid, GridOrEmpty, GridSize, MapPixels, MapPixelsParallel, RasterTile2D};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, NumCast};
use std::ops::{Add, Div, Mul, Sub};

pub fn scale_convert<In, Out>(pixel_value: In, scale_with: In, offset_by: In) -> Option<Out>
where
    In: NumCast + Copy + 'static + Sub<Output = In> + Div<Output = In>,
    Out: NumCast + Copy + 'static,
{
    NumCast::from((pixel_value - offset_by) / scale_with)
}

pub fn unscale_convert<In, Out>(pixel_value: In, scale_with: Out, offset_by: Out) -> Option<Out>
where
    In: NumCast + Copy + 'static,
    Out: NumCast + Copy + 'static + Add<Output = Out> + Mul<Output = Out>,
{
    num_traits::cast::<In, Out>(pixel_value).map(|v| v * scale_with + offset_by)
}

pub fn scale_convert_checked<In, Out>(pixel_value: In, scale_with: In, offset_by: In) -> Option<Out>
where
    In: NumCast + Copy + 'static + CheckedSub<Output = In> + CheckedDiv<Output = In>,
    Out: NumCast + Copy + 'static,
{
    pixel_value
        .checked_sub(&offset_by)
        .and_then(|f| f.checked_div(&scale_with))
        .and_then(NumCast::from)
}

pub fn unscale_convert_checked<In, Out>(
    pixel_value: In,
    scale_with: Out,
    offset_by: Out,
) -> Option<Out>
where
    In: NumCast + Copy + 'static,
    Out: NumCast + Copy + 'static + CheckedAdd<Output = Out> + CheckedMul<Output = Out>,
{
    num_traits::cast::<In, Out>(pixel_value)
        .and_then(|f| f.checked_mul(&scale_with))
        .and_then(|f| f.checked_add(&offset_by))
}

pub trait ScaleConvert<Out> {
    fn scale_convert(self, scale_with: Self, offset_by: Self) -> Option<Out>;
}

macro_rules! impl_scale_conv {
    ($T:ty, $conv:ident) => {
        impl<Out> ScaleConvert<Out> for $T
        where
            Out: Copy + 'static + NumCast,
        {
            #[inline]
            fn scale_convert(self, scale_with: Self, offset_by: Self) -> Option<Out> {
                $conv(self, scale_with, offset_by)
            }
        }
    };
}

impl_scale_conv!(u8, scale_convert_checked);
impl_scale_conv!(u16, scale_convert_checked);
impl_scale_conv!(u32, scale_convert_checked);
impl_scale_conv!(u64, scale_convert_checked);
impl_scale_conv!(i8, scale_convert_checked);
impl_scale_conv!(i16, scale_convert_checked);
impl_scale_conv!(i32, scale_convert_checked);
impl_scale_conv!(i64, scale_convert_checked);
impl_scale_conv!(f32, scale_convert);
impl_scale_conv!(f64, scale_convert);

pub trait UnscaleConvert<In>
where
    Self: Sized,
{
    fn unscale_convert(value: In, scale_with: Self, offset_by: Self) -> Option<Self>;
}

macro_rules! impl_unscale_conv {
    ($T:ty, $conv:ident) => {
        impl<In> UnscaleConvert<In> for $T
        where
            In: NumCast + Copy + 'static,
        {
            #[inline]
            fn unscale_convert(value: In, scale_with: Self, offset_by: Self) -> Option<Self> {
                $conv(value, scale_with, offset_by)
            }
        }
    };
}

impl_unscale_conv!(u8, unscale_convert_checked);
impl_unscale_conv!(u16, unscale_convert_checked);
impl_unscale_conv!(u32, unscale_convert_checked);
impl_unscale_conv!(u64, unscale_convert_checked);
impl_unscale_conv!(i8, unscale_convert_checked);
impl_unscale_conv!(i16, unscale_convert_checked);
impl_unscale_conv!(i32, unscale_convert_checked);
impl_unscale_conv!(i64, unscale_convert_checked);
impl_unscale_conv!(f32, unscale_convert);
impl_unscale_conv!(f64, unscale_convert);

pub trait ScaleConvertElements<In, Out> {
    type Output;

    fn scale_convert_elements(
        self,
        scale_with: In,
        offset_by: In,
        out_no_data: Out,
    ) -> Self::Output;
}

impl<In, Out, G> ScaleConvertElements<In, Out> for Grid<G, In>
where
    In: ScaleConvert<Out> + 'static + Copy + PartialEq,
    Out: 'static + Copy,
    G: GridSize + Clone,
{
    type Output = Grid<G, Out>;

    fn scale_convert_elements(
        self,
        scale_with: In,
        offset_by: In,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels(|p| p.scale_convert(scale_with, offset_by), out_no_data)
    }
}

impl<In, Out, G> ScaleConvertElements<In, Out> for GridOrEmpty<G, In>
where
    In: ScaleConvert<Out> + 'static + Copy + PartialEq,
    Out: 'static + Copy,
    G: GridSize + Clone,
{
    type Output = GridOrEmpty<G, Out>;

    fn scale_convert_elements(
        self,
        scale_with: In,
        offset_by: In,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels(|p| p.scale_convert(scale_with, offset_by), out_no_data)
    }
}

impl<In, Out> ScaleConvertElements<In, Out> for RasterTile2D<In>
where
    In: ScaleConvert<Out> + 'static + Copy + PartialEq,
    Out: 'static + Copy,
{
    type Output = RasterTile2D<Out>;

    fn scale_convert_elements(
        self,
        scale_with: In,
        offset_by: In,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels(|p| p.scale_convert(scale_with, offset_by), out_no_data)
    }
}

pub trait ScaleConvertElementsParallel<In, Out> {
    type Output;

    fn scale_convert_elements_parallel(
        self,
        scale_with: In,
        offset_by: In,
        out_no_data: Out,
    ) -> Self::Output;
}

impl<In, Out, G> ScaleConvertElementsParallel<In, Out> for Grid<G, In>
where
    In: ScaleConvert<Out> + 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync,
    G: GridSize + Clone + Send + Sync,
{
    type Output = Grid<G, Out>;

    fn scale_convert_elements_parallel(
        self,
        scale_with: In,
        offset_by: In,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels_parallel(|p| p.scale_convert(scale_with, offset_by), out_no_data)
    }
}

impl<In, Out, G> ScaleConvertElementsParallel<In, Out> for GridOrEmpty<G, In>
where
    In: ScaleConvert<Out> + 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync,
    G: GridSize + Clone + Send + Sync,
{
    type Output = GridOrEmpty<G, Out>;

    fn scale_convert_elements_parallel(
        self,
        scale_with: In,
        offset_by: In,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels(|p| p.scale_convert(scale_with, offset_by), out_no_data)
    }
}

impl<In, Out> ScaleConvertElementsParallel<In, Out> for RasterTile2D<In>
where
    In: ScaleConvert<Out> + 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync,
{
    type Output = RasterTile2D<Out>;

    fn scale_convert_elements_parallel(
        self,
        scale_with: In,
        offset_by: In,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels_parallel(|p| p.scale_convert(scale_with, offset_by), out_no_data)
    }
}

pub trait UnscaleConvertElements<In, Out> {
    type Output;

    fn unscale_convert_elements(
        self,
        scale_with: Out,
        offset_by: Out,
        out_no_data: Out,
    ) -> Self::Output;
}

impl<In, Out, G> UnscaleConvertElements<In, Out> for Grid<G, In>
where
    Out: UnscaleConvert<In> + 'static + Copy + PartialEq,
    In: 'static + Copy + PartialEq,
    G: GridSize + Clone,
{
    type Output = Grid<G, Out>;

    fn unscale_convert_elements(
        self,
        scale_with: Out,
        offset_by: Out,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels(
            |p| Out::unscale_convert(p, scale_with, offset_by),
            out_no_data,
        )
    }
}

impl<In, Out, G> UnscaleConvertElements<In, Out> for GridOrEmpty<G, In>
where
    Out: UnscaleConvert<In> + 'static + Copy + PartialEq,
    In: 'static + Copy + PartialEq,
    G: GridSize + Clone,
{
    type Output = GridOrEmpty<G, Out>;

    fn unscale_convert_elements(
        self,
        scale_with: Out,
        offset_by: Out,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels(
            |p| Out::unscale_convert(p, scale_with, offset_by),
            out_no_data,
        )
    }
}

impl<In, Out> UnscaleConvertElements<In, Out> for RasterTile2D<In>
where
    Out: UnscaleConvert<In> + 'static + Copy + PartialEq,
    In: 'static + Copy + PartialEq,
    Out: 'static + Copy,
{
    type Output = RasterTile2D<Out>;

    fn unscale_convert_elements(
        self,
        scale_with: Out,
        offset_by: Out,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels(
            |p| Out::unscale_convert(p, scale_with, offset_by),
            out_no_data,
        )
    }
}

pub trait UnscaleConvertElementsParallel<In, Out> {
    type Output;

    fn unscale_convert_elements_parallel(
        self,
        scale_with: Out,
        offset_by: Out,
        out_no_data: Out,
    ) -> Self::Output;
}

impl<In, Out, G> UnscaleConvertElementsParallel<In, Out> for Grid<G, In>
where
    Out: UnscaleConvert<In> + 'static + Copy + PartialEq + Send + Sync,
    In: 'static + Copy + PartialEq + Send + Sync,
    G: GridSize + Clone + Send + Sync,
{
    type Output = Grid<G, Out>;

    fn unscale_convert_elements_parallel(
        self,
        scale_with: Out,
        offset_by: Out,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels_parallel(
            |p| Out::unscale_convert(p, scale_with, offset_by),
            out_no_data,
        )
    }
}

impl<In, Out, G> UnscaleConvertElementsParallel<In, Out> for GridOrEmpty<G, In>
where
    Out: UnscaleConvert<In> + 'static + Copy + PartialEq + Send + Sync,
    In: 'static + Copy + PartialEq + Send + Sync,
    G: GridSize + Clone + Send + Sync,
{
    type Output = GridOrEmpty<G, Out>;

    fn unscale_convert_elements_parallel(
        self,
        scale_with: Out,
        offset_by: Out,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels_parallel(
            |p| Out::unscale_convert(p, scale_with, offset_by),
            out_no_data,
        )
    }
}

impl<In, Out> UnscaleConvertElementsParallel<In, Out> for RasterTile2D<In>
where
    Out: UnscaleConvert<In> + 'static + Copy + PartialEq + Send + Sync,
    In: 'static + Copy + PartialEq + Send + Sync,
    Out: 'static + Copy + Send + Sync,
{
    type Output = RasterTile2D<Out>;

    fn unscale_convert_elements_parallel(
        self,
        scale_with: Out,
        offset_by: Out,
        out_no_data: Out,
    ) -> Self::Output {
        self.map_pixels_parallel(
            |p| Out::unscale_convert(p, scale_with, offset_by),
            out_no_data,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unscale_u8() {
        assert_eq!(unscale_convert_checked(5 as u8, 10, 2), Some(52 as u8));
        assert_eq!(unscale_convert_checked(5 as u8, 1000, 2), Some(5002 as u16));
        assert_eq!(
            unscale_convert_checked(5 as u8, 10_000_000, 2),
            Some(50_000_002 as u32)
        );
        assert_eq!(
            unscale_convert_checked(5 as u8, 1_000_000_000, 2),
            Some(5_000_000_002 as u64)
        );
    }

    #[test]
    fn unscale_u8_overflow() {
        assert_eq!(
            unscale_convert_checked(5 as u8, u8::MAX, 1),
            None as Option<u8>
        );
        assert_eq!(
            unscale_convert_checked(5 as u8, 1, u16::MAX),
            None as Option<u16>
        );
        assert_eq!(
            unscale_convert_checked(5 as u8, u32::MAX, 1),
            None as Option<u32>
        );
        assert_eq!(
            unscale_convert_checked(5 as u8, 1, u64::MAX),
            None as Option<u64>
        );
    }

    #[test]
    fn unscale_u8_u8_over() {
        let res = unscale_convert_checked(10, 100, 2);
        assert_eq!(res, None as Option<u8>);
    }
}
