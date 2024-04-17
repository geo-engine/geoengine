use crate::{
    engine::{
        MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors, RasterResultDescriptor,
        StaticMetaData, VectorColumnInfo, VectorResultDescriptor,
    },
    error::{self, Error},
    source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetaDataRegular,
        GdalSourceTimePlaceholder, OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType,
        OgrSourceErrorSpec, TimeReference,
    },
    test_data,
    util::Result,
};
use gdal::{Dataset, DatasetOptions, DriverManager};
use geoengine_datatypes::{
    collections::VectorDataType,
    dataset::{DataId, DatasetId, NamedData},
    hashmap,
    primitives::{
        BoundingBox2D, CacheTtlSeconds, ContinuousMeasurement, DateTimeParseFormat,
        FeatureDataType, Measurement, TimeGranularity, TimeInstance, TimeInterval, TimeStep,
        VectorQueryRectangle,
    },
    raster::{BoundedGrid, GeoTransform, GridBoundingBox2D, GridShape2D, RasterDataType},
    spatial_reference::SpatialReference,
    util::Identifier,
};
use itertools::Itertools;
use log::Level::Debug;
use log::{debug, log_enabled};
use snafu::ResultExt;
use std::{
    collections::HashSet,
    convert::TryInto,
    hash::BuildHasher,
    path::{Path, PathBuf},
    str::FromStr,
};

// TODO: move test helper somewhere else?
pub fn create_ndvi_meta_data() -> GdalMetaDataRegular {
    create_ndvi_meta_data_with_cache_ttl(CacheTtlSeconds::default())
}

pub fn create_ndvi_meta_data_cropped_to_valid_webmercator_bounds() -> GdalMetaDataRegular {
    create_ndvi_meta_data_with_cache_ttl(CacheTtlSeconds::default())
}

#[allow(clippy::missing_panics_doc)]
pub fn create_ndvi_meta_data_with_cache_ttl(cache_ttl: CacheTtlSeconds) -> GdalMetaDataRegular {
    let no_data_value = Some(0.); // TODO: is it really 0?
    GdalMetaDataRegular {
        data_time: TimeInterval::new_unchecked(
            TimeInstance::from_str("2014-01-01T00:00:00.000Z")
                .expect("it should only be used in tests"),
            TimeInstance::from_str("2014-07-01T00:00:00.000Z")
                .expect("it should only be used in tests"),
        ),
        step: TimeStep {
            granularity: TimeGranularity::Months,
            step: 1,
        },
        time_placeholders: hashmap! {
            "%_START_TIME_%".to_string() => GdalSourceTimePlaceholder {
                format: DateTimeParseFormat::custom("%Y-%m-%d".to_string()),
                reference: TimeReference::Start,
            },
        },
        params: GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        },
        result_descriptor: RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: Some(TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z")
                    .expect("it should only be used in tests"),
                TimeInstance::from_str("2014-07-01T00:00:00.000Z")
                    .expect("it should only be used in tests"),
            )),
            geo_transform_x: GeoTransform::new((0., 0.).into(), 0.1, -0.1),
            pixel_bounds_x: GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            bands: RasterBandDescriptors::new_single_band(),
        },
        cache_ttl,
    }
}

#[allow(clippy::missing_panics_doc)]
pub fn create_ndvi_meta_data_cropped_to_valid_webmercator_bounds_with_cache_ttl(
    cache_ttl: CacheTtlSeconds,
) -> GdalMetaDataRegular {
    let no_data_value = Some(0.); // TODO: is it really 0?
    GdalMetaDataRegular {
        data_time: TimeInterval::new_unchecked(
            TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
            TimeInstance::from_str("2014-07-01T00:00:00.000Z").unwrap(),
        ),
        step: TimeStep {
            granularity: TimeGranularity::Months,
            step: 1,
        },
        time_placeholders: hashmap! {
            "%_START_TIME_%".to_string() => GdalSourceTimePlaceholder {
                format: DateTimeParseFormat::custom("%Y-%m-%d".to_string()),
                reference: TimeReference::Start,
            },
        },
        params: GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 85.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1700,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        },
        result_descriptor: RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            geo_transform_x: GeoTransform::new((0., 0.).into(), 0.1, -0.1),
            pixel_bounds_x: GridBoundingBox2D::new([-850, -1800], [-845, -1799]).unwrap(),
            time: Some(TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-07-01T00:00:00.000Z").unwrap(),
            )),
            bands: vec![RasterBandDescriptor {
                name: "ndvi".to_string(),
                measurement: Measurement::Continuous(ContinuousMeasurement {
                    measurement: "vegetation".to_string(),
                    unit: None,
                }),
            }]
            .try_into()
            .expect("it should only be used in tests"),
        },
        cache_ttl,
    }
}

// TODO: move test helper somewhere else?
pub fn add_ndvi_dataset(ctx: &mut MockExecutionContext) -> NamedData {
    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("ndvi");
    ctx.add_meta_data(id, name.clone(), Box::new(create_ndvi_meta_data()));
    name
}

pub fn add_ndvi_dataset_cropped_to_valid_webmercator_bounds(
    ctx: &mut MockExecutionContext,
) -> NamedData {
    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("ndvi_crop_y_85");
    ctx.add_meta_data(
        id,
        name.clone(),
        Box::new(create_ndvi_meta_data_cropped_to_valid_webmercator_bounds()),
    );
    name
}

#[allow(clippy::missing_panics_doc)]
pub fn create_ports_meta_data(
) -> StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle> {
    StaticMetaData {
        loading_info: OgrSourceDataset {
            file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
            layer_name: "ne_10m_ports".to_string(),
            data_type: Some(VectorDataType::MultiPoint),
            time: OgrSourceDatasetTimeType::None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: None,
                x: String::new(),
                y: None,
                int: vec!["natlscale".to_string()],
                float: vec!["scalerank".to_string()],
                text: vec![
                    "featurecla".to_string(),
                    "name".to_string(),
                    "website".to_string(),
                ],
                bool: vec![],
                datetime: vec![],
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
            default_geometry: None,
            cache_ttl: CacheTtlSeconds::default(),
        },
        result_descriptor: VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReference::epsg_4326().into(),
            columns: [
                (
                    "natlscale".to_string(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Int,
                        measurement: Measurement::Unitless,
                    },
                ),
                (
                    "scalerank".to_string(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Int,
                        measurement: Measurement::Unitless,
                    },
                ),
                (
                    "featurecla".to_string(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Int,
                        measurement: Measurement::Unitless,
                    },
                ),
                (
                    "name".to_string(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Int,
                        measurement: Measurement::Unitless,
                    },
                ),
                (
                    "website".to_string(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Int,
                        measurement: Measurement::Unitless,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            time: Some(TimeInterval::default()),
            bbox: Some(BoundingBox2D::new_unchecked(
                (-171.75795, -54.809_444).into(),
                (179.309_364, 78.226_111).into(),
            )),
        },
        phantom: std::marker::PhantomData,
    }
}

pub fn add_ports_dataset(ctx: &mut MockExecutionContext) -> NamedData {
    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("ne_10m_ports");
    ctx.add_meta_data(id, name.clone(), Box::new(create_ports_meta_data()));
    name
}

/// Opens a Gdal Dataset with the given `path`.
/// Other crates should use this method for Gdal Dataset access as a workaround to avoid strange errors.
pub fn gdal_open_dataset(path: &Path) -> Result<Dataset> {
    gdal_open_dataset_ex(path, DatasetOptions::default())
}

/// Opens a Gdal Dataset with the given `path` and `dataset_options`.
/// Other crates should use this method for Gdal Dataset access as a workaround to avoid strange errors.
pub fn gdal_open_dataset_ex(path: &Path, dataset_options: DatasetOptions) -> Result<Dataset> {
    let dataset_options = {
        let mut dataset_options = dataset_options;
        dataset_options.open_flags |= gdal::GdalOpenFlags::GDAL_OF_VERBOSE_ERROR;
        dataset_options
    };

    Dataset::open_ex(path, dataset_options).context(error::Gdal)
}

/// Create a `RasterResultDescriptor` for the given `band` and `dataset`. If the raster data type is
/// unknown, the default is F64 unless it is otherwise specified by `default_data_type`. If the data
/// type is a complex floating point type, an error is returned
pub fn raster_descriptor_from_dataset(
    dataset: &Dataset,
    band: isize,
) -> Result<RasterResultDescriptor> {
    let rasterband = &dataset.rasterband(band)?;

    let spatial_ref: SpatialReference =
        dataset.spatial_ref()?.try_into().context(error::DataType)?;

    let data_type = RasterDataType::from_gdal_data_type(rasterband.band_type())
        .map_err(|_| Error::GdalRasterDataTypeNotSupported)?;

    let data_geo_transfrom = GeoTransform::from(dataset.geo_transform()?);
    let data_shape = GridShape2D::new([dataset.raster_size().1, dataset.raster_size().0]);

    Ok(RasterResultDescriptor {
        data_type,
        spatial_reference: spatial_ref.into(),
        time: None,
        geo_transform_x: data_geo_transfrom,
        pixel_bounds_x: data_shape.bounding_box(),
        bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
            "band".into(), // TODO: derive better name?
            measurement_from_rasterband(dataset, band)?,
        )])?,
    })
}

// a version of `raster_descriptor_from_dataset` that does not read the sref from the dataset but takes it as an argument
pub fn raster_descriptor_from_dataset_and_sref(
    dataset: &Dataset,
    band: isize,
    spatial_ref: SpatialReference,
) -> Result<RasterResultDescriptor> {
    let rasterband = &dataset.rasterband(band)?;

    let data_type = RasterDataType::from_gdal_data_type(rasterband.band_type())
        .map_err(|_| Error::GdalRasterDataTypeNotSupported)?;

    let data_geo_transfrom = GeoTransform::from(dataset.geo_transform()?);
    let data_shape = GridShape2D::new([dataset.raster_size().1, dataset.raster_size().0]);

    Ok(RasterResultDescriptor {
        data_type,
        spatial_reference: spatial_ref.into(),
        time: None,
        geo_transform_x: data_geo_transfrom,
        pixel_bounds_x: data_shape.bounding_box(),
        bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
            "band".into(), // TODO derive better name?
            measurement_from_rasterband(dataset, band)?,
        )])?,
    })
}

// TODO: use https://github.com/georust/gdal/pull/271 when merged and released
fn measurement_from_rasterband(dataset: &Dataset, band: isize) -> Result<Measurement> {
    unsafe fn _string(raw_ptr: *const std::os::raw::c_char) -> String {
        let c_str = std::ffi::CStr::from_ptr(raw_ptr);
        c_str.to_string_lossy().into_owned()
    }

    unsafe fn _last_null_pointer_err(method_name: &'static str) -> gdal::errors::GdalError {
        let last_err_msg = _string(gdal_sys::CPLGetLastErrorMsg());
        gdal_sys::CPLErrorReset();
        gdal::errors::GdalError::NullPointer {
            method_name,
            msg: last_err_msg,
        }
    }

    let unit: String = unsafe {
        // taken from `pub fn rasterband(&self, band_index: isize) -> Result<RasterBand>`
        let c_band = gdal_sys::GDALGetRasterBand(dataset.c_dataset(), band as std::os::raw::c_int);
        if c_band.is_null() {
            Err(_last_null_pointer_err("GDALGetRasterBand"))?;
        }

        let str_ptr = gdal_sys::GDALGetRasterUnitType(c_band);
        Result::<String>::Ok(_string(str_ptr))
    }?;

    if unit.trim().is_empty() || unit == "no unit" {
        return Ok(Measurement::Unitless);
    }

    // TODO: how to check if the measurement is contiuous vs. classification?

    Ok(Measurement::continuous(String::new(), Some(unit)))
}

/// Create `GdalDatasetParameters` from the infos in the given `dataset` and its `band`.
/// `path` is the location of the actual data, `band_out` allows optionally specifying a different
/// band in the resulting parameters, otherwise `band` is used.
pub fn gdal_parameters_from_dataset(
    dataset: &Dataset,
    band: usize,
    path: &Path,
    band_out: Option<usize>,
    open_options: Option<Vec<String>>,
) -> Result<GdalDatasetParameters> {
    let rasterband = &dataset.rasterband(band as isize)?;

    Ok(GdalDatasetParameters {
        file_path: PathBuf::from(path),
        rasterband_channel: band_out.unwrap_or(band),
        geo_transform: dataset.geo_transform().context(error::Gdal)?.into(),
        file_not_found_handling: FileNotFoundHandling::Error,
        no_data_value: rasterband.no_data_value(),
        properties_mapping: None,
        width: rasterband.x_size(),
        height: rasterband.y_size(),
        gdal_open_options: open_options,
        gdal_config_options: None,
        allow_alphaband_as_mask: true,
        retry: None,
    })
}

/// This method registers all GDAL drivers from the `drivers` list.
/// It also de-registers all other drivers.
///
/// It makes sure to call `GDALAllRegister` at least once.
/// Unfortunately, calling this method does not prevent registering other drivers afterwards.
///
pub fn register_gdal_drivers_from_list<S: BuildHasher>(mut drivers: HashSet<String, S>) {
    // this calls `GDALAllRegister` internally
    let number_of_drivers = DriverManager::count();
    let mut start_index = 0;

    for _ in 0..number_of_drivers {
        let Ok(driver) = DriverManager::get_driver(start_index) else {
            // in the unlikely case that we cannot fetch a driver, we will just skip it
            continue;
        };

        // do not unregister the drivers we want to keep
        if drivers.remove(&driver.short_name()) {
            // driver was found in list --> keep it
            start_index += 1;
        } else {
            // driver was not found in list --> unregister the driver
            DriverManager::deregister_driver(&driver);
        }
    }

    if !drivers.is_empty() && log_enabled!(Debug) {
        let mut drivers: Vec<String> = drivers.into_iter().collect();
        drivers.sort();
        let remaining_drivers = drivers.into_iter().join(", ");
        debug!("Could not register drivers: {remaining_drivers}");
    }
}
