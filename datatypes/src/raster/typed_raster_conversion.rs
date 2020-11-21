use super::{GridArray, GridSize, Pixel, TypedGridArray};

pub trait TypedRasterConversion<D>
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel;

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel;
}

impl<D> TypedRasterConversion<D> for i8
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_i8()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::I8(raster)
    }
}

impl<D> TypedRasterConversion<D> for i16
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_i16()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::I16(raster)
    }
}

impl<D> TypedRasterConversion<D> for i32
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_i32()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::I32(raster)
    }
}

impl<D> TypedRasterConversion<D> for i64
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_i64()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::I64(raster)
    }
}

impl<D> TypedRasterConversion<D> for u8
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_u8()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::U8(raster)
    }
}

impl<D> TypedRasterConversion<D> for u16
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_u16()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::U16(raster)
    }
}

impl<D> TypedRasterConversion<D> for u32
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_u32()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::U32(raster)
    }
}

impl<D> TypedRasterConversion<D> for u64
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_u64()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::U64(raster)
    }
}

impl<D> TypedRasterConversion<D> for f32
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_f32()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::F32(raster)
    }
}

impl<D> TypedRasterConversion<D> for f64
where
    D: GridSize,
{
    fn get_raster(raster: TypedGridArray<D>) -> Option<GridArray<D, Self>>
    where
        Self: Pixel,
    {
        raster.get_f64()
    }

    fn get_typed_raster(raster: GridArray<D, Self>) -> TypedGridArray<D>
    where
        Self: Pixel,
    {
        TypedGridArray::<D>::F64(raster)
    }
}
