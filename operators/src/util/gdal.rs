use geoengine_datatypes::{
    dataset::{DataSetId, InternalDataSetId},
    primitives::{BoundingBox2D, Measurement, TimeGranularity, TimeStep},
    raster::{GeoTransform, RasterDataType},
    spatial_reference::SpatialReference,
    util::Identifier,
};

use crate::{
    engine::{MockExecutionContext, RasterResultDescriptor},
    source::{FileNotFoundHandling, GdalDataSetParameters, GdalMetaDataRegular},
};

// TODO: better way for determining raster directory
fn raster_dir() -> std::path::PathBuf {
    let mut current_path = std::env::current_dir().unwrap();

    if current_path.ends_with("services") {
        current_path = current_path.join("../operators");
    }

    if !current_path.ends_with("operators") {
        current_path = current_path.join("operators");
    }

    current_path = current_path.join("test-data/raster");
    current_path
}

// TODO: move test helper somewhere else?
pub fn create_ndvi_meta_data() -> GdalMetaDataRegular {
    GdalMetaDataRegular {
        start: 1_388_534_400_000.into(),
        step: TimeStep {
            granularity: TimeGranularity::Months,
            step: 1,
        },
        placeholder: "%%%_START_TIME_%%%".to_string(),
        time_format: "%Y-%m-%d".to_string(),
        params: GdalDataSetParameters {
            file_path: raster_dir().join("modis_ndvi/MOD13A2_M_NDVI_%%%_START_TIME_%%%.TIFF"),
            rasterband_channel: 1,
            geo_transform: GeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            bbox: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
            file_not_found_handling: FileNotFoundHandling::NoData,
        },
        result_descriptor: RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        },
    }
}

// TODO: move test helper somewhere else?
pub fn add_ndvi_data_set(ctx: &mut MockExecutionContext) -> DataSetId {
    let id: DataSetId = InternalDataSetId::new().into();
    ctx.add_meta_data(id.clone(), Box::new(create_ndvi_meta_data()));
    id
}
