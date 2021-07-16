use std::path::Path;

use gdal::{Dataset, DatasetOptions};
use geoengine_datatypes::{
    dataset::{DatasetId, InternalDatasetId},
    primitives::{Measurement, TimeGranularity, TimeInstance, TimeStep},
    raster::{GeoTransform, GridShape2D, RasterDataType},
    spatial_reference::SpatialReference,
    util::Identifier,
};
use snafu::ResultExt;

use crate::{
    engine::{MockExecutionContext, RasterResultDescriptor},
    error,
    source::{FileNotFoundHandling, GdalDatasetParameters, GdalMetaDataRegular},
    util::Result,
};

/// # Panics
/// If current dir is not accessible
// TODO: better way for determining raster directory
pub fn raster_dir() -> std::path::PathBuf {
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
#[allow(clippy::missing_panics_doc)]
pub fn create_ndvi_meta_data() -> GdalMetaDataRegular {
    let no_data_value = Some(0.); // TODO: is it really 0?
    GdalMetaDataRegular {
        start: TimeInstance::from_millis(1_388_534_400_000).unwrap(),
        step: TimeStep {
            granularity: TimeGranularity::Months,
            step: 1,
        },
        placeholder: "%%%_START_TIME_%%%".to_string(),
        time_format: "%Y-%m-%d".to_string(),
        params: GdalDatasetParameters {
            file_path: raster_dir().join("modis_ndvi/MOD13A2_M_NDVI_%%%_START_TIME_%%%.TIFF"),
            rasterband_channel: 1,
            geo_transform: GeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            grid_shape: GridShape2D::new([1800, 3600]),
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value,
            properties_mapping: None,
        },
        result_descriptor: RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
            no_data_value,
        },
    }
}

// TODO: move test helper somewhere else?
pub fn add_ndvi_dataset(ctx: &mut MockExecutionContext) -> DatasetId {
    let id: DatasetId = InternalDatasetId::new().into();
    ctx.add_meta_data(id.clone(), Box::new(create_ndvi_meta_data()));
    id
}

/// Opens a Gdal Dataset with the given `path`.
/// Other crates should use this method for Gdal Dataset access as a workaround to avoid strange errors.
pub fn gdal_open_dataset(path: &Path) -> Result<Dataset> {
    Dataset::open(&path).context(error::Gdal)
}

/// Opens a Gdal Dataset with the given `path` and `dataset_options`.
/// Other crates should use this method for Gdal Dataset access as a workaround to avoid strange errors.
pub fn gdal_open_dataset_ex(path: &Path, dataset_options: DatasetOptions) -> Result<Dataset> {
    Dataset::open_ex(path, dataset_options).context(error::Gdal)
}
