use super::{Grid2D, GridOrEmpty2D, GridSize, Pixel, RasterTile2D, TypedGrid2D};
use crate::{
    primitives::{CacheHint, TimeInterval},
    raster::{
        BoundedGrid, ChangeGridBounds, FromPrimitive, GeoTransform, Grid, GridBoundingBox2D,
        GridIdx2D, GridOrEmpty, GridShape, GridShapeAccess, MaskedGrid, RasterDataType,
        raster_properties,
    },
    spatial_reference::SpatialReferenceOption,
};
use arrow::{
    array::{Array, ArrayRef, PrimitiveBuilder},
    datatypes::{
        Field, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Schema,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
    ipc::{
        reader::FileReader,
        writer::{FileWriter, IpcWriteOptions},
    },
    record_batch::RecordBatch,
};
use arrow_array::PrimitiveArray;
use arrow_schema::ArrowError;
use snafu::Snafu;
use std::{collections::HashMap, io::Cursor, sync::Arc};
use strum::IntoStaticStr;

#[derive(Debug, Snafu, IntoStaticStr)]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum RasterArrowConversionError {
    #[snafu(display("Unsupported data type for raster conversion: {datatype}"))]
    UnsupportedDataType {
        datatype: String,
    },
    #[snafu(display("Arrow IPC error during raster conversion: {source}"))]
    ArrowInternal {
        source: ArrowError,
    },

    NoRecordBatchFound,

    #[snafu(display("Expected metadata key {key} not found during raster conversion"))]
    MetadataNotFound {
        key: &'static str,
    },

    #[snafu(display("Error during deserialization of json: {source}"))]
    DeserializationError {
        source: serde_json::Error,
    },

    RasterDataFieldNotFound,

    #[snafu(display("Grid bounds from metadata do not match the grid shape of the data"))]
    GridBoundsDoNotMatchData {
        bounds: GridBoundingBox2D,
    },
}

impl From<ArrowError> for RasterArrowConversionError {
    fn from(value: ArrowError) -> Self {
        RasterArrowConversionError::ArrowInternal { source: value }
    }
}

impl From<serde_json::Error> for RasterArrowConversionError {
    fn from(value: serde_json::Error) -> Self {
        RasterArrowConversionError::DeserializationError { source: value }
    }
}

pub type Result<T> = std::result::Result<T, RasterArrowConversionError>;

pub const RASTER_DATA_FIELD_NAME: &str = "data";
pub const GEO_TRANSFORM_KEY: &str = "geoTransform";
pub const TIME_KEY: &str = "time";
pub const SPATIAL_REF_KEY: &str = "spatialReference";
pub const BAND_KEY: &str = "band";
pub const RASTER_PROPERTIES: &str = "rasterProperties";
pub const TILE_POSITION: &str = "tilePosition";
pub const GRID_BOUNDS: &str = "gridBounds";

/// Converts a grid with properties into an Arrow `RecordBatch`, including the grid data as an Arrow array and the properties as metadata.
/// The grid bounds are also included in the metadata to allow reconstructing the grid from the array.
///
/// # Errors
/// Returns an error if the grid bounds do not match the grid shape, or if the properties cannot be serialized to JSON for inclusion in the metadata.
/// The conversion of the grid data to an Arrow array should not fail, as it is a straightforward mapping of the grid values and validity mask to an Arrow primitive array.
///
/// # Panics
/// Panics if the grid bounds cannot be mapped to a serde-compatible format for inclusion in the metadata, or if the properties cannot be mapped to a serde-compatible format for inclusion in the metadata. This should not happen as long as the grid bounds and properties are properly defined and implement the necessary traits for serialization.
///
pub fn grid_with_properties_to_arrow_record_batch<P: Pixel>(
    grid: GridOrEmpty<GridBoundingBox2D, P>,
    properties: &raster_properties::RasterProperties,
) -> Result<RecordBatch> {
    let grid_bounds = grid.bounding_box();

    let array = grid_2d_to_arrow_array(grid.unbounded()); // conversion should be cheap here

    let metadata: HashMap<String, String> = [
        (
            GRID_BOUNDS.to_string(),
            serde_json::to_string(&grid_bounds).expect("grid bounds should be mappable to serde"),
        ),
        (
            RASTER_PROPERTIES.to_string(),
            serde_json::to_string(&properties)
                .expect("tile properties should be mappable to serde"),
        ),
    ]
    .into();

    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new(
            RASTER_DATA_FIELD_NAME,
            array.data_type().clone(),
            true,
        )],
        metadata,
    ));

    let record_batch = RecordBatch::try_new(schema, vec![array])?;

    Ok(record_batch)
}

pub fn record_batch_to_bytes(record_batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut file_writer = FileWriter::try_new_with_options(
        Vec::new(),
        &record_batch.schema(),
        IpcWriteOptions::default(),
    )?;
    file_writer.write(record_batch)?;
    file_writer.finish()?;

    Ok(file_writer.into_inner()?)
}

pub fn bytes_to_record_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let cursor = Cursor::new(bytes);
    let reader = FileReader::try_new(cursor, None)?;

    // the writer only writes one batch
    if let Some(batch) = reader.flatten().next() {
        return Ok(batch);
    }

    Err(RasterArrowConversionError::NoRecordBatchFound)
}

pub fn raster_tile_2d_to_arrow_ipc_file_for_ipc_channel<P: Pixel>(
    tile: RasterTile2D<P>,
) -> Result<Vec<u8>> {
    let record_batch = raster_tile_2d_to_arrow_record_batch_impl(tile, None)?;

    record_batch_to_bytes(&record_batch)
}

pub fn arrow_ipc_file_to_raster_tile_2d<P>(tile: &[u8]) -> Result<RasterTile2D<P>>
where
    P: Pixel,
{
    arrow_ipc_file_to_raster_tile_2d_impl(tile)
}

pub fn arrow_ipc_file_to_raster_tile_2d_for_ipc_channel<P>(tile: &[u8]) -> Result<RasterTile2D<P>>
where
    P: Pixel,
{
    arrow_ipc_file_to_raster_tile_2d_impl(tile)
}

fn arrow_ipc_file_to_record_batch(tile: &[u8]) -> Result<RecordBatch> {
    bytes_to_record_batch(tile)
}

pub fn arrow_record_batch_to_grid_with_properties<P: Pixel>(
    record_batch: &RecordBatch,
) -> Result<(
    GridOrEmpty<GridBoundingBox2D, P>,
    raster_properties::RasterProperties,
)> {
    let schema = record_batch.schema();
    let metadata = schema.metadata();

    let grid_bounds: GridBoundingBox2D = serde_json::from_str(
        metadata
            .get(GRID_BOUNDS)
            .ok_or_else(|| RasterArrowConversionError::MetadataNotFound { key: GRID_BOUNDS })?
            .as_str(),
    )?;

    let properties: raster_properties::RasterProperties = serde_json::from_str(
        metadata
            .get(RASTER_PROPERTIES)
            .ok_or_else(|| RasterArrowConversionError::MetadataNotFound {
                key: RASTER_PROPERTIES,
            })?
            .as_str(),
    )?;

    let field_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == RASTER_DATA_FIELD_NAME)
        .ok_or_else(|| RasterArrowConversionError::RasterDataFieldNotFound)?;

    let arr = record_batch.column(field_idx);

    let grid = arrow_array_to_grid_2d::<P>(arr, grid_bounds.grid_shape())?
        .set_grid_bounds(grid_bounds)
        .map_err(|_| RasterArrowConversionError::GridBoundsDoNotMatchData {
            bounds: grid_bounds,
        })?;

    Ok((grid, properties))
}

fn arrow_record_batch_to_raster_tile_2d<P: Pixel>(
    record_batch: &RecordBatch,
) -> Result<RasterTile2D<P>> {
    let (grid, properties) = arrow_record_batch_to_grid_with_properties(record_batch)?;

    let geo_transform: GeoTransform = serde_json::from_str(
        record_batch
            .schema()
            .metadata()
            .get(GEO_TRANSFORM_KEY)
            .ok_or_else(|| RasterArrowConversionError::MetadataNotFound {
                key: GEO_TRANSFORM_KEY,
            })?
            .as_str(),
    )?;

    let time: TimeInterval = serde_json::from_str(
        record_batch
            .schema()
            .metadata()
            .get(TIME_KEY)
            .ok_or_else(|| RasterArrowConversionError::MetadataNotFound { key: TIME_KEY })?
            .as_str(),
    )?;

    let band: usize = serde_json::from_str(
        record_batch
            .schema()
            .metadata()
            .get(BAND_KEY)
            .ok_or_else(|| RasterArrowConversionError::MetadataNotFound { key: BAND_KEY })?
            .as_str(),
    )?;

    let tile_position: GridIdx2D = serde_json::from_str(
        record_batch
            .schema()
            .metadata()
            .get(TILE_POSITION)
            .ok_or_else(|| RasterArrowConversionError::MetadataNotFound { key: TILE_POSITION })?
            .as_str(),
    )?;

    let cache_hint = CacheHint::default();

    let raster_tile_2d = RasterTile2D::new_with_properties(
        time,
        tile_position,
        band as u32,
        geo_transform,
        grid.unbounded(), // remove the grid bounds, as they are not needed anymore and would just add overhead to the tile
        properties,
        cache_hint,
    );
    Ok(raster_tile_2d)
}

fn arrow_ipc_file_to_raster_tile_2d_impl<P>(tile: &[u8]) -> Result<RasterTile2D<P>>
where
    P: Pixel,
{
    let record_batch = arrow_ipc_file_to_record_batch(tile)?;
    let raster_tile_2d = arrow_record_batch_to_raster_tile_2d(&record_batch)?;
    Ok(raster_tile_2d)
}

fn arrow_array_to_grid_2d<P>(
    arr: &ArrayRef,
    size: GridShape<[usize; 2]>,
) -> Result<GridOrEmpty2D<P>>
where
    P: Pixel,
{
    // in case of empty array
    if arr.null_count() == arr.len() {
        return Ok(GridOrEmpty::new_empty_shape(size));
    }

    let (values, validity_mask) = arrow_array_ref_to_values_and_validity::<P>(arr);

    let data = Grid::new(size, values).map_err(|_| {
        RasterArrowConversionError::GridBoundsDoNotMatchData {
            bounds: size.bounding_box(),
        }
    })?;
    let validity = Grid::new(size, validity_mask).map_err(|_| {
        RasterArrowConversionError::GridBoundsDoNotMatchData {
            bounds: size.bounding_box(),
        }
    })?;

    Ok(GridOrEmpty::new_grid(
        MaskedGrid::new(data, validity).expect("both grids ara valid and have the same shape"),
    ))
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::unwrap_used)]
fn arrow_array_ref_to_values_and_validity<P: Pixel>(arr: &ArrayRef) -> (Vec<P>, Vec<bool>) {
    let validity = (0..arr.len()).map(|i| arr.is_valid(i)).collect();
    match arr.data_type() {
        arrow::datatypes::DataType::UInt8 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::UInt8Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::UInt16 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::UInt16Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::UInt32 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::UInt32Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::UInt64 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::UInt64Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::Int8 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::Int8Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::Int16 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::Int16Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::Int32 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::Int32Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::Int64Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::Float32 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::Float32Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        arrow::datatypes::DataType::Float64 => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<arrow::datatypes::Float64Type>>()
                .unwrap();
            let values = arr.values().to_vec();
            (
                values.iter().map(|x| FromPrimitive::from_(*x)).collect(),
                validity,
            )
        }
        _ => panic!("unsupported data type"), // remove panic and return some other type
    }
}

pub fn raster_tile_2d_to_arrow_ipc_file<P: Pixel>(
    tile: RasterTile2D<P>,
    spatial_ref: SpatialReferenceOption,
) -> Result<Vec<u8>> {
    let record_batch = raster_tile_2d_to_arrow_record_batch_impl(tile, Some(spatial_ref))?;
    record_batch_to_bytes(&record_batch)
}

fn raster_tile_2d_to_arrow_record_batch_impl<P: Pixel>(
    tile: RasterTile2D<P>,
    spatial_ref: Option<SpatialReferenceOption>,
) -> Result<RecordBatch> {
    let geo_transform = tile.tile_geo_transform();
    let grid_shape = tile.grid_array.grid_shape();
    let band = tile.band;
    let time = tile.time;

    let mut rb = grid_with_properties_to_arrow_record_batch(
        tile.grid_array
            .set_grid_bounds(grid_shape.bounding_box())
            .expect("grid shape should match the grid bounds"),
        &tile.properties,
    )?;

    rb.schema_metadata_mut().insert(
        GEO_TRANSFORM_KEY.to_string(),
        serde_json::to_string(&geo_transform).unwrap_or_default(),
    );
    rb.schema_metadata_mut().insert(
        TIME_KEY.to_string(),
        serde_json::to_string(&time).unwrap_or_default(),
    );
    rb.schema_metadata_mut()
        .insert(BAND_KEY.to_string(), band.to_string());

    if let Some(spatial_ref) = spatial_ref {
        rb.schema_metadata_mut()
            .insert(SPATIAL_REF_KEY.to_string(), spatial_ref.to_string());
    }

    rb.schema_metadata_mut().insert(
        TILE_POSITION.to_string(),
        serde_json::to_string(&tile.tile_position).unwrap_or_default(),
    );

    Ok(rb)
}

fn grid_2d_to_arrow_array<P: Pixel>(grid: GridOrEmpty2D<P>) -> ArrayRef {
    let number_of_values = grid.number_of_elements();

    let GridOrEmpty::Grid(grid) = grid else {
        // if the grid is empty, we create a null array instead of materializing the data
        let array = arrow::array::new_null_array(&arrow_data_type::<P>(), number_of_values);
        return Arc::new(array);
    };

    let value_grid: TypedGrid2D = grid.inner_grid.into();
    let validity_grid: Grid2D<bool> = grid.validity_mask;

    let array: ArrayRef = match value_grid {
        TypedGrid2D::U8(grid) => {
            let mut builder = PrimitiveBuilder::<UInt8Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::U16(grid) => {
            let mut builder = PrimitiveBuilder::<UInt16Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::U32(grid) => {
            let mut builder = PrimitiveBuilder::<UInt32Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::U64(grid) => {
            let mut builder = PrimitiveBuilder::<UInt64Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::I8(grid) => {
            let mut builder = PrimitiveBuilder::<Int8Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::I16(grid) => {
            let mut builder = PrimitiveBuilder::<Int16Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::I32(grid) => {
            let mut builder = PrimitiveBuilder::<Int32Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::I64(grid) => {
            let mut builder = PrimitiveBuilder::<Int64Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::F32(grid) => {
            let mut builder = PrimitiveBuilder::<Float32Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
        TypedGrid2D::F64(grid) => {
            let mut builder = PrimitiveBuilder::<Float64Type>::with_capacity(number_of_values);
            builder.append_values(&grid.data, &validity_grid.data);
            Arc::new(builder.finish())
        }
    };

    array
}

const fn arrow_data_type<P: Pixel>() -> arrow::datatypes::DataType {
    match P::TYPE {
        RasterDataType::U8 => arrow::datatypes::DataType::UInt8,
        RasterDataType::U16 => arrow::datatypes::DataType::UInt16,
        RasterDataType::U32 => arrow::datatypes::DataType::UInt32,
        RasterDataType::U64 => arrow::datatypes::DataType::UInt64,
        RasterDataType::I8 => arrow::datatypes::DataType::Int8,
        RasterDataType::I16 => arrow::datatypes::DataType::Int16,
        RasterDataType::I32 => arrow::datatypes::DataType::Int32,
        RasterDataType::I64 => arrow::datatypes::DataType::Int64,
        RasterDataType::F32 => arrow::datatypes::DataType::Float32,
        RasterDataType::F64 => arrow::datatypes::DataType::Float64,
    }
}

#[cfg(test)]
mod tests {
    use arrow::ipc::reader::FileReader;

    use super::*;
    use crate::{
        primitives::{CacheHint, TimeInterval},
        raster::{EmptyGrid2D, GridIndexAccessMut, MaskedGrid2D, TileInformation},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    #[test]
    fn test_grid_2d_to_arrow_array() {
        let values = Grid2D::new_filled([4, 4].into(), 3);
        let validity = Grid2D::new_filled([4, 4].into(), true);

        let masked_grid = MaskedGrid2D::new(values.clone(), validity).unwrap();

        let array = grid_2d_to_arrow_array::<u8>(masked_grid.into());

        assert_eq!(array.len(), 16);
        assert_eq!(array.null_count(), 0);

        // with nulls

        let mut validity = Grid2D::new_filled([4, 4].into(), true);
        validity.set_at_grid_index([1, 1], false).unwrap();
        validity.set_at_grid_index([2, 2], false).unwrap();
        validity.set_at_grid_index([3, 3], false).unwrap();

        let masked_grid = MaskedGrid2D::new(values, validity).unwrap();

        let array = grid_2d_to_arrow_array::<u8>(masked_grid.into());

        assert_eq!(array.len(), 16);
        assert_eq!(array.null_count(), 3);
        assert_eq!(*array.data_type(), arrow_data_type::<u8>());
        assert_eq!(*array.data_type(), arrow::datatypes::DataType::UInt8);
    }

    #[test]
    fn test_grid_2d_to_arrow_array_u16() {
        let values = Grid2D::new_filled([4, 4].into(), 3);
        let validity = Grid2D::new_filled([4, 4].into(), true);

        let masked_grid = MaskedGrid2D::new(values.clone(), validity).unwrap();

        let array = grid_2d_to_arrow_array::<u16>(masked_grid.into());

        assert_eq!(array.len(), 16);
        assert_eq!(array.null_count(), 0);

        // with nulls

        let mut validity = Grid2D::new_filled([4, 4].into(), true);
        validity.set_at_grid_index([1, 1], false).unwrap();
        validity.set_at_grid_index([2, 2], false).unwrap();
        validity.set_at_grid_index([3, 3], false).unwrap();

        let masked_grid = MaskedGrid2D::new(values, validity).unwrap();

        let array = grid_2d_to_arrow_array::<u16>(masked_grid.into());

        assert_eq!(array.len(), 16);
        assert_eq!(array.null_count(), 3);
        assert_eq!(*array.data_type(), arrow_data_type::<u16>());
        assert_eq!(*array.data_type(), arrow::datatypes::DataType::UInt16);
    }

    #[test]
    fn test_raster_tile_2d_to_arrow_ipc_file() {
        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let bytes =
            raster_tile_2d_to_arrow_ipc_file(raster_tile, SpatialReference::epsg_4326().into())
                .unwrap();

        let mut reader = FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        let schema = reader.schema();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&schema.metadata()[GEO_TRANSFORM_KEY])
                .unwrap(),
            serde_json::json!({"originCoordinate":{"x":0.0,"y":0.0}, "xPixelSize": 1., "yPixelSize": -1.})
        );
        // TODO: add check for grid bounds metadta
        assert_eq!(
            schema.metadata()[GRID_BOUNDS],
            "{\"min\":[0,0],\"max\":[2,1]}"
        );
        //assert_eq!(schema.metadata()[X_SIZE_KEY], "2");
        //assert_eq!(schema.metadata()[Y_SIZE_KEY], "3");
        assert_eq!(
            schema.metadata()[TIME_KEY],
            "{\"start\":-8334601228800000,\"end\":8210266876799999}"
        );
        assert_eq!(schema.metadata()[SPATIAL_REF_KEY], "EPSG:4326");

        let data = reader.next().unwrap().unwrap();

        let values = data
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .values()
            .to_vec();

        assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);

        assert!(reader.next().is_none()); // only one batch
    }

    #[test]
    fn test_raster_tile_2d_to_arrow_ipc_file_of_empty_tile() {
        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            EmptyGrid2D::<f64>::new([3, 2].into()).into(),
            CacheHint::default(),
        );

        let bytes =
            raster_tile_2d_to_arrow_ipc_file(raster_tile, SpatialReference::epsg_4326().into())
                .unwrap();

        let mut reader = FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        let schema = reader.schema();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&schema.metadata()[GEO_TRANSFORM_KEY])
                .unwrap(),
            serde_json::json!({"originCoordinate":{"x":0.0,"y":0.0}, "xPixelSize": 1., "yPixelSize": -1.})
        );
        assert_eq!(
            schema.metadata()[GRID_BOUNDS],
            "{\"min\":[0,0],\"max\":[2,1]}"
        );
        assert_eq!(
            schema.metadata()[TIME_KEY],
            "{\"start\":-8334601228800000,\"end\":8210266876799999}"
        );
        assert_eq!(schema.metadata()[SPATIAL_REF_KEY], "EPSG:4326"); // TODO (low): Does this also crash?

        let data = reader.next().unwrap().unwrap();

        let nulls = data
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .null_count();

        assert_eq!(nulls, 6);

        assert!(reader.next().is_none()); // only one batch
    }

    #[test]
    fn test_arrow_ipc_file_to_raster_tile_2d_empty() {
        let original = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            EmptyGrid2D::<f32>::new([3, 2].into()).into(),
            CacheHint::default(),
        );

        let bytes = raster_tile_2d_to_arrow_ipc_file(
            original.clone(),
            SpatialReference::epsg_4326().into(),
        )
        .unwrap();
        let restored: RasterTile2D<f32> = arrow_ipc_file_to_raster_tile_2d(&bytes).unwrap();

        assert!(restored.grid_array.is_empty());
        assert_eq!(original.grid_array, restored.grid_array);
        assert_eq!(original.properties, restored.properties);
        assert_eq!(original.tile_position, restored.tile_position);
    }

    #[test]
    fn test_arrow_ipc_file_to_raster_tile_2d_for_ipc_channel() {
        let original = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [1, 2].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            5,
            Grid2D::new([3, 2].into(), vec![10_u8, 20, 30, 40, 50, 60])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let bytes = raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(original.clone()).unwrap();
        let restored: RasterTile2D<u8> =
            arrow_ipc_file_to_raster_tile_2d_for_ipc_channel(&bytes).unwrap();

        assert_eq!(original.time, restored.time);
        assert_eq!(original.band, restored.band);
        assert_eq!(original.grid_array, restored.grid_array);

        assert_eq!(original.properties, restored.properties);
        assert_eq!(original.tile_position, restored.tile_position);
    }
}
