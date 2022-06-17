pub use self::data_type::{
    DynamicRasterDataType, FromPrimitive, Pixel, RasterDataType, StaticRasterDataType, TypedValue,
};
pub use self::empty_grid::{EmptyGrid, EmptyGrid1D, EmptyGrid2D, EmptyGrid3D};
pub use self::geo_transform::{GdalGeoTransform, GeoTransform};
pub use self::grid::{
    grid_idx_iter_2d, Grid, Grid1D, Grid2D, Grid3D, GridShape, GridShape1D, GridShape2D,
    GridShape3D,
};
pub use self::grid_bounds::{
    GridBoundingBox, GridBoundingBox1D, GridBoundingBox2D, GridBoundingBox3D,
};
pub use self::grid_index::{GridIdx, GridIdx1D, GridIdx2D, GridIdx3D};
pub use self::grid_or_empty::{GridOrEmpty, GridOrEmpty1D, GridOrEmpty2D, GridOrEmpty3D};
pub use self::grid_traits::{
    BoundedGrid, GridBounds, GridContains, GridIndexAccess, GridIndexAccessMut, GridIntersection,
    GridSize, GridSpaceToLinearSpace, GridStep, MaskedGridIndexAccess, MaskedGridIndexAccessMut,
};
pub use self::grid_typed::{TypedGrid, TypedGrid2D, TypedGrid3D};
pub use self::operations::{
    blit::Blit, convert_data_type::ConvertDataType, convert_data_type::ConvertDataTypeParallel,
    grid_blit::GridBlit, interpolation::Bilinear, interpolation::InterpolationAlgorithm,
    interpolation::NearestNeighbor,
};
pub use self::raster_tile::{
    BaseTile, MaterializedRasterTile, MaterializedRasterTile2D, MaterializedRasterTile3D,
    RasterTile, RasterTile2D, RasterTile3D,
};
pub use self::tiling::{TileInformation, TilingSpecification, TilingStrategy};
pub use self::typed_raster_conversion::TypedRasterConversion;
pub use self::typed_raster_tile::{TypedRasterTile2D, TypedRasterTile3D};
pub use self::{
    grid_traits::ChangeGridBounds, grid_traits::GridShapeAccess, grid_traits::NoDataValue,
};
use super::primitives::{SpatialBounded, TemporalBounded};
use crate::primitives::Coordinate2D;
use crate::util::Result;
pub use masked_grid::{MaskedGrid, MaskedGrid1D, MaskedGrid2D, MaskedGrid3D};
pub use operations::map_elements::{
    MapElements, MapElementsOrMask, MapElementsOrMaskParallel, MapElementsParallel,
};
pub use raster_properties::{
    RasterProperties, RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey,
};

mod data_type;
mod empty_grid;
mod geo_transform;
mod grid;
mod grid_bounds;
mod grid_index;
mod grid_or_empty;
mod grid_traits;
mod grid_typed;
mod macros_raster;
mod macros_raster_tile;
mod masked_grid;
mod operations;
mod raster_properties;
mod raster_tile;
mod tiling;
mod typed_raster_conversion;
mod typed_raster_tile;

pub trait Raster<D: GridSize, T: Pixel>:
    SpatialBounded
    + TemporalBounded
    + NoDataValue<NoDataType = T>
    + GridShapeAccess<ShapeArray = D::ShapeArray>
    + GeoTransformAccess
{
    type DataContainer;
    /// returns a reference to the data container used to hold the pixels / cells of the raster
    fn data_container(&self) -> &Self::DataContainer;
}

pub trait GeoTransformAccess {
    /// returns a reference to the geo transform describing the origin of the raster and the pixel size
    fn geo_transform(&self) -> GeoTransform;
}

pub trait CoordinatePixelAccess<P>
where
    P: Pixel,
{
    fn pixel_value_at_coord(&self, coordinate: Coordinate2D) -> Result<Option<P>>;

    fn pixel_value_at_coord_unchecked(&self, coordinate: Coordinate2D) -> Option<P>;
}
