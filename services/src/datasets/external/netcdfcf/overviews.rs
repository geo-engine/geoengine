use super::{error, parse_time_coverage, NetCdfCf4DProviderError};
use crate::{
    datasets::external::netcdfcf::{gdalmd::MdGroup, NetCdfCfDataProvider},
    error::BoxedResultExt,
    util::config::get_config_element,
};
use gdal::{raster::RasterCreationOption, Dataset, DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::primitives::{TimeInstance, TimeStep};
use geoengine_operators::{
    source::GdalMetadataNetCdfCf,
    util::gdal::{
        gdal_open_dataset_ex, gdal_parameters_from_dataset, raster_descriptor_from_dataset,
    },
};
use log::debug;
use snafu::{OptionExt, ResultExt};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

pub const METADATA_FILE_NAME: &str = "metadata.json";

#[derive(Debug, Clone)]
struct NetCdfGroup {
    name: String,
    groups: Vec<NetCdfGroup>,
    arrays: Vec<String>,
}

#[derive(Debug, Clone)]
struct ConversionMetadata {
    pub dataset_in: String,
    pub dataset_out: PathBuf,
}

impl NetCdfGroup {
    fn flatten(&self) -> Vec<Vec<String>> {
        let mut out_paths = Vec::new();

        for group in &self.groups {
            out_paths.extend(group.flatten_mut(Vec::new()));
        }

        for array in &self.arrays {
            out_paths.push(vec![array.clone()]);
        }

        out_paths
    }

    fn flatten_mut(&self, mut path_vec: Vec<String>) -> Vec<Vec<String>> {
        let mut out_paths = Vec::new();

        path_vec.push(self.name.clone());

        for group in &self.groups {
            out_paths.extend(group.flatten_mut(path_vec.clone()));
        }

        for array in &self.arrays {
            let mut path_vec = path_vec.clone();
            path_vec.push(array.clone());
            out_paths.push(path_vec);
        }

        out_paths
    }

    fn conversion_metadata(
        &self,
        file_path: &Path,
        out_root_path: &Path,
    ) -> Vec<ConversionMetadata> {
        let in_path = file_path.to_string_lossy();
        let mut metadata = Vec::new();

        for data_path in self.flatten() {
            let md_path = data_path.join("/");
            metadata.push(ConversionMetadata {
                dataset_in: format!("NETCDF:\"{in_path}\":/{md_path}"),
                dataset_out: out_root_path.join(md_path).with_extension("tiff"),
            });
        }

        metadata
    }
}

trait NetCdfVisitor {
    fn group_tree(&self) -> Result<NetCdfGroup>;
}

impl NetCdfVisitor for MdGroup<'_> {
    fn group_tree(&self) -> Result<NetCdfGroup> {
        let mut groups = Vec::new();
        for subgroup_name in self.group_names() {
            let subgroup = self.open_group(&subgroup_name)?;
            groups.push(subgroup.group_tree()?);
        }

        let dimension_names: HashSet<String> = self
            .dimension_names()
            .map_err(|source| NetCdfCf4DProviderError::CannotReadDimensions { source })?
            .into_iter()
            .collect();

        let mut arrays = Vec::new();
        for array_name in self.array_names() {
            // filter out arrays that are actually dimensions
            if dimension_names.contains(&array_name) {
                continue;
            }

            arrays.push(array_name.to_string());
        }

        Ok(NetCdfGroup {
            name: self.name.clone(),
            groups,
            arrays,
        })
    }
}

pub fn create_overviews(
    provider_path: &Path,
    dataset_path: &Path,
    overview_path: &Path,
) -> Result<()> {
    let file_path = provider_path.join(dataset_path);
    let out_folder_path = overview_path.join(dataset_path);

    // check that file does not "escape" the provider path
    if let Err(source) = file_path.strip_prefix(provider_path) {
        return Err(NetCdfCf4DProviderError::DatasetIsNotInProviderPath { source });
    }
    // check that file does not "escape" the overview path
    if let Err(source) = out_folder_path.strip_prefix(overview_path) {
        return Err(NetCdfCf4DProviderError::DatasetIsNotInProviderPath { source });
    }

    let dataset = gdal_open_dataset_ex(
        &file_path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
            allowed_drivers: Some(&["netCDF"]),
            open_options: None,
            sibling_files: None,
        },
    )
    .boxed_context(error::CannotOpenNetCdfDataset)?;

    let root_group = MdGroup::from_dataset(&dataset)?;

    let group_tree = root_group.group_tree()?;

    store_metadata(provider_path, dataset_path, &out_folder_path)?;

    let time_coverage = time_coverage(&root_group)?;

    for conversion in group_tree.conversion_metadata(&file_path, &out_folder_path) {
        index_subdataset(&conversion, &time_coverage)?;
    }

    Ok(())
}

fn time_coverage(root_group: &MdGroup) -> Result<TimeCoverage> {
    let start = root_group
        .attribute_as_string("time_coverage_start")
        .context(error::MissingTimeCoverageStart)?;
    let end = root_group
        .attribute_as_string("time_coverage_end")
        .context(error::MissingTimeCoverageEnd)?;
    let step = root_group
        .attribute_as_string("time_coverage_resolution")
        .context(error::MissingTimeCoverageResolution)?;

    let (start, end, step) = parse_time_coverage(&start, &end, &step)?;

    Ok(TimeCoverage { start, end, step })
}

struct TimeCoverage {
    start: TimeInstance,
    end: TimeInstance,
    step: TimeStep,
}

fn store_metadata(provider_path: &Path, dataset_path: &Path, out_folder_path: &Path) -> Result<()> {
    let out_file_path = out_folder_path.join(METADATA_FILE_NAME);

    if out_file_path.exists() {
        debug!("Skipping metadata generation: {}", dataset_path.display());
        return Ok(());
    }

    debug!("Creating metadata: {}", dataset_path.display());

    let metadata = NetCdfCfDataProvider::build_netcdf_tree(provider_path, None, dataset_path)?;

    fs::create_dir_all(out_folder_path).boxed_context(error::CannotCreateOverviews)?;

    let file = File::create(out_file_path).boxed_context(error::CannotWriteMetadataFile)?;

    let mut writer = BufWriter::new(file);

    writer
        .write_all(
            serde_json::to_string(&metadata)
                .boxed_context(error::CannotWriteMetadataFile)?
                .as_bytes(),
        )
        .boxed_context(error::CannotWriteMetadataFile)?;

    Ok(())
}

fn index_subdataset(conversion: &ConversionMetadata, time_coverage: &TimeCoverage) -> Result<()> {
    const COG_BLOCK_SIZE: &str = "512";
    const COMPRESSION_FORMAT: &str = "LZW";
    const COMPRESSION_LEVEL: &str = "9";

    if conversion.dataset_out.exists() {
        debug!("Skipping conversion: {}", conversion.dataset_out.display());
        return Ok(());
    }

    debug!("Indexing conversion: {}", conversion.dataset_out.display());

    let subdataset = gdal_open_dataset_ex(
        Path::new(&conversion.dataset_in),
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY,
            allowed_drivers: Some(&["netCDF"]),
            open_options: None,
            sibling_files: None,
        },
    )
    .boxed_context(error::CannotOpenNetCdfSubdataset)?;

    fs::create_dir_all(
        conversion
            .dataset_out
            .parent()
            .context(error::CannotReadInputFileDir {
                path: conversion.dataset_out.clone(),
            })?,
    )
    .boxed_context(error::CannotCreateOverviews)?;

    let gdal_num_threads = get_config_element::<crate::util::config::Gdal>()
        .boxed_context(error::CannotCreateOverviews)?
        .compression_num_threads;
    let num_threads = gdal_num_threads.to_string();

    let cog_driver = gdal::Driver::get("COG").boxed_context(error::CannotCreateOverviews)?;
    let options = vec![
        RasterCreationOption {
            key: "COMPRESS",
            value: COMPRESSION_FORMAT,
        },
        RasterCreationOption {
            key: "LEVEL",
            value: COMPRESSION_LEVEL,
        },
        RasterCreationOption {
            key: "NUM_THREADS",
            value: &num_threads,
        },
        RasterCreationOption {
            key: "BLOCKSIZE",
            value: COG_BLOCK_SIZE,
        },
        RasterCreationOption {
            key: "BIGTIFF",
            value: "IF_SAFER", // TODO: test if this suffices
        },
    ];

    subdataset
        .create_copy(&cog_driver, &conversion.dataset_out, &options)
        .boxed_context(error::CannotCreateOverviews)?;

    let loading_info = generate_loading_info(&subdataset, &conversion.dataset_out, time_coverage)?;

    let loading_info_file = File::create(conversion.dataset_out.with_extension("json"))
        .boxed_context(error::CannotWriteMetadataFile)?;

    let mut writer = BufWriter::new(loading_info_file);

    writer
        .write_all(
            serde_json::to_string(&loading_info)
                .boxed_context(error::CannotWriteMetadataFile)?
                .as_bytes(),
        )
        .boxed_context(error::CannotWriteMetadataFile)?;

    Ok(())
}

fn generate_loading_info(
    dataset: &Dataset,
    overview_dataset_path: &Path,
    time_coverage: &TimeCoverage,
) -> Result<GdalMetadataNetCdfCf> {
    const SAMPLE_BAND: usize = 1;

    let result_descriptor = raster_descriptor_from_dataset(dataset, 1, None)
        .boxed_context(error::CannotGenerateLoadingInfo)?;

    let params = gdal_parameters_from_dataset(
        dataset,
        SAMPLE_BAND,
        overview_dataset_path,
        Some(0), // we calculate offsets in our source
        None,
    )
    .boxed_context(error::CannotGenerateLoadingInfo)?;

    Ok(GdalMetadataNetCdfCf {
        result_descriptor,
        params,
        start: time_coverage.start,
        end: time_coverage.end,
        step: time_coverage.step,
        band_offset: 0, // must be changed if there are other dimensions then geo & time
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datasets::external::netcdfcf::{
        NetCdfEntity, NetCdfGroup as FullNetCdfGroup, NetCdfOverview,
    };
    use geoengine_datatypes::{
        operations::image::{Colorizer, RgbaColor},
        primitives::{DateTime, Measurement, TimeGranularity, TimeInterval},
        raster::RasterDataType,
        spatial_reference::SpatialReference,
        test_data,
        util::gdal::hide_gdal_errors,
    };
    use geoengine_operators::{
        engine::RasterResultDescriptor,
        source::{FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters},
    };
    use std::io::BufReader;

    #[test]
    fn test_generate_loading_info() {
        hide_gdal_errors();

        let netcdf_path_str = format!(
            "NETCDF:\"{}\":/metric_1/ebv_cube",
            test_data!("netcdf4d/dataset_m.nc").display()
        );
        let netcdf_path = Path::new(&netcdf_path_str);

        let dataset = gdal_open_dataset_ex(
            netcdf_path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_READONLY,
                allowed_drivers: Some(&["netCDF"]),
                open_options: None,
                sibling_files: None,
            },
        )
        .unwrap();

        let loading_info = generate_loading_info(
            &dataset,
            Path::new("foo/bar.tif"),
            &TimeCoverage {
                start: DateTime::new_utc(2020, 1, 1, 0, 0, 0).into(),
                end: DateTime::new_utc(2021, 1, 1, 0, 0, 0).into(),
                step: TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
            },
        )
        .unwrap();

        assert_eq!(
            loading_info,
            GdalMetadataNetCdfCf {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: Some(-9999.0),
                },
                params: GdalDatasetParameters {
                    file_path: Path::new("foo/bar.tif").into(),
                    rasterband_channel: 0,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: (50., 55.).into(),
                        x_pixel_size: 1.,
                        y_pixel_size: -1.,
                    },
                    width: 5,
                    height: 5,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value: Some(-9999.0),
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                },
                start: DateTime::new_utc(2020, 1, 1, 0, 0, 0).into(),
                end: DateTime::new_utc(2021, 1, 1, 0, 0, 0).into(),
                step: TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
                band_offset: 0,
            }
        );
    }

    #[test]
    fn test_index_subdataset() {
        hide_gdal_errors();

        let dataset_in = format!(
            "NETCDF:\"{}\":/metric_1/ebv_cube",
            test_data!("netcdf4d/dataset_m.nc").display()
        );

        let tempdir = tempfile::tempdir().unwrap();
        let dataset_out = tempdir.path().join("out.tif");

        assert!(!dataset_out.exists());

        index_subdataset(
            &ConversionMetadata {
                dataset_in,
                dataset_out: dataset_out.clone(),
            },
            &TimeCoverage {
                start: DateTime::new_utc(2020, 1, 1, 0, 0, 0).into(),
                end: DateTime::new_utc(2021, 1, 1, 0, 0, 0).into(),
                step: TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
            },
        )
        .unwrap();

        assert!(dataset_out.exists());
    }

    #[test]
    fn test_store_metadata() {
        hide_gdal_errors();

        let out_folder = tempfile::tempdir().unwrap();

        let metadata_file_path = out_folder.path().join(METADATA_FILE_NAME);

        assert!(!metadata_file_path.exists());

        store_metadata(
            test_data!("netcdf4d"),
            Path::new("dataset_m.nc"),
            out_folder.path(),
        )
        .unwrap();

        assert!(metadata_file_path.exists());

        let file = File::open(metadata_file_path).unwrap();
        let mut reader = BufReader::new(file);

        let metadata: NetCdfOverview = serde_json::from_reader(&mut reader).unwrap();

        assert_eq!(
            metadata,
            NetCdfOverview {
                file_name: "dataset_m.nc".into(),
                title: "Test dataset metric".to_string(),
                summary: "CFake description of test dataset with metric.".to_string(),
                spatial_reference: SpatialReference::epsg_4326(),
                groups: vec![
                    FullNetCdfGroup {
                        name: "metric_1".to_string(),
                        title: "Random metric 1".to_string(),
                        description: "Randomly created data".to_string(),
                        data_type: Some(RasterDataType::I16),
                        unit: "".to_string(),
                        groups: vec![]
                    },
                    FullNetCdfGroup {
                        name: "metric_2".to_string(),
                        title: "Random metric 2".to_string(),
                        description: "Randomly created data".to_string(),
                        data_type: Some(RasterDataType::I16),
                        unit: "".to_string(),
                        groups: vec![]
                    }
                ],
                entities: vec![
                    NetCdfEntity {
                        id: 0,
                        name: "entity01".to_string(),
                    },
                    NetCdfEntity {
                        id: 1,
                        name: "entity02".to_string(),
                    },
                    NetCdfEntity {
                        id: 2,
                        name: "entity03".to_string(),
                    }
                ],
                time: TimeInterval::new(
                    DateTime::new_utc(2000, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2003, 1, 1, 0, 0, 0)
                )
                .unwrap(),
                time_step: TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1
                },
                colorizer: Colorizer::LinearGradient {
                    breakpoints: vec![
                        (0.0, RgbaColor::new(0, 0, 0, 255)).try_into().unwrap(),
                        (255.0, RgbaColor::new(255, 255, 255, 255))
                            .try_into()
                            .unwrap(),
                    ],
                    no_data_color: RgbaColor::new(0, 0, 0, 0),
                    default_color: RgbaColor::new(0, 0, 0, 0)
                },
            }
        );
    }

    #[test]
    fn test_create_overviews() {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        create_overviews(
            test_data!("netcdf4d"),
            Path::new("dataset_m.nc"),
            overview_folder.path(),
        )
        .unwrap();

        let dataset_folder = overview_folder.path().join("dataset_m.nc");

        assert!(dataset_folder.is_dir());

        assert!(dataset_folder.join("metadata.json").exists());

        assert!(dataset_folder.join("metric_1/ebv_cube.tiff").exists());
        assert!(dataset_folder.join("metric_1/ebv_cube.json").exists());

        assert!(dataset_folder.join("metric_2/ebv_cube.tiff").exists());
        assert!(dataset_folder.join("metric_2/ebv_cube.json").exists());
    }
}
