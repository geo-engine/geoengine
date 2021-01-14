mod data_type;
mod geo_transform;
mod grid;
mod grid_bounds;
mod grid_index;
mod grid_traits;

mod grid_typed;
mod macros_raster;
mod macros_raster_tile;
mod operations;
mod raster_tile;
mod tiling;
mod typed_raster_conversion;
mod typed_raster_tile;

pub use self::data_type::{
    DynamicRasterDataType, FromPrimitive, Pixel, RasterDataType, StaticRasterDataType, TypedValue,
};
pub use self::geo_transform::{GdalGeoTransform, GeoTransform};

pub use self::operations::{blit::Blit, grid_blit::GridBlit};

pub use self::grid_index::{GridIdx, GridIdx1D, GridIdx2D, GridIdx3D};
pub use self::grid_traits::{
    BoundedGrid, GridBounds, GridContains, GridIndexAccess, GridIndexAccessMut, GridIntersection,
    GridSize, GridSpaceToLinearSpace,
};
pub use self::grid_typed::{TypedGrid, TypedGrid2D, TypedGrid3D};
pub use self::typed_raster_conversion::TypedRasterConversion;
pub use self::typed_raster_tile::{TypedRasterTile2D, TypedRasterTile3D};
use super::primitives::{SpatialBounded, TemporalBounded};
pub use grid::{Grid, Grid1D, Grid2D, Grid3D, GridShape, GridShape1D, GridShape2D, GridShape3D};
pub use grid_bounds::{GridBoundingBox, GridBoundingBox1D, GridBoundingBox2D, GridBoundingBox3D};
pub use raster_tile::{RasterTile, RasterTile2D, RasterTile3D};
pub use tiling::{TileInformation, TilingStrategy};

pub trait Raster<D, T: Pixel, C>: SpatialBounded + TemporalBounded {
    /// returns the grid dimension object of type D: `GridDimension`
    fn dimension(&self) -> D;
    /// returns the optional  no-data value used for the raster
    fn no_data_value(&self) -> Option<T>;
    /// returns a reference to the data container used to hold the pixels / cells of the raster
    fn data_container(&self) -> &C;
    /// returns a reference to the geo transform describing the origin of the raster and the pixel size
    fn geo_transform(&self) -> GeoTransform;
}

pub trait CoordinatePixelAccess<T>
where
    T: Pixel,
{
    fn pixel_value_at_coord(&self, coordinate: (f64, f64)) -> T;
}
