use super::{Grid2D, GridOrEmpty, GridOrEmpty2D, GridSize, Pixel, RasterTile2D, TypedGrid2D};
use crate::{raster::RasterDataType, spatial_reference::SpatialReferenceOption, util::Result};
use arrow::{
    array::{Array, ArrayRef, PrimitiveBuilder},
    datatypes::{
        Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    ipc::writer::{FileWriter, IpcWriteOptions},
    record_batch::RecordBatch,
};
use std::{collections::HashMap, sync::Arc};

pub const RASTER_DATA_FIELD_NAME: &str = "data";
pub const GEO_TRANSFORM_KEY: &str = "geoTransform";
pub const X_SIZE_KEY: &str = "xSize";
pub const Y_SIZE_KEY: &str = "ySize";
pub const TIME_KEY: &str = "time";
pub const SPATIAL_REF_KEY: &str = "spatialReference";

pub fn raster_tile_2d_to_arrow_ipc_file<P: Pixel>(
    tile: RasterTile2D<P>,
    spatial_ref: SpatialReferenceOption,
) -> Result<Vec<u8>> {
    let record_batch = raster_tile_2d_to_arrow_record_batch(tile, spatial_ref)?;

    let mut file_writer = FileWriter::try_new_with_options(
        Vec::new(),
        &record_batch.schema(),
        IpcWriteOptions::default().try_with_compression(None)?, //Some(CompressionType::LZ4_FRAME))?, // TODO: enable compression when pyarrow >= 12. Also: make this configurable.
    )?;
    file_writer.write(&record_batch)?;
    file_writer.finish()?;

    Ok(file_writer.into_inner()?)
}

fn raster_tile_2d_to_arrow_record_batch<P: Pixel>(
    tile: RasterTile2D<P>,
    spatial_ref: SpatialReferenceOption,
) -> Result<RecordBatch> {
    let metadata: HashMap<String, String> = [
        (
            GEO_TRANSFORM_KEY.to_string(),
            serde_json::to_string(&tile.tile_geo_transform()).unwrap_or_default(),
        ),
        (
            X_SIZE_KEY.to_string(),
            tile.grid_array.axis_size_x().to_string(),
        ),
        (
            Y_SIZE_KEY.to_string(),
            tile.grid_array.axis_size_y().to_string(),
        ),
        (
            TIME_KEY.to_string(),
            serde_json::to_string(&tile.time).unwrap_or_default(),
        ),
        (SPATIAL_REF_KEY.to_string(), spatial_ref.to_string()),
    ]
    .into();

    let array = grid_2d_to_arrow_array(tile.grid_array);

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
        primitives::TimeInterval,
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
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
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
        assert_eq!(schema.metadata()[X_SIZE_KEY], "2");
        assert_eq!(schema.metadata()[Y_SIZE_KEY], "3");
        assert_eq!(
            schema.metadata()[TIME_KEY],
            "{\"start\":-8334632851200000,\"end\":8210298412799999}"
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
            EmptyGrid2D::<f64>::new([3, 2].into()).into(),
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
        assert_eq!(schema.metadata()[X_SIZE_KEY], "2");
        assert_eq!(schema.metadata()[Y_SIZE_KEY], "3");
        assert_eq!(
            schema.metadata()[TIME_KEY],
            "{\"start\":-8334632851200000,\"end\":8210298412799999}"
        );
        assert_eq!(schema.metadata()[SPATIAL_REF_KEY], "EPSG:4326");

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
}
