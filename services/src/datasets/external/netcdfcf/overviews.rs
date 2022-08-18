use super::{error, time_coverage, NetCdfCf4DProviderError, TimeCoverage};
use crate::{
    datasets::external::netcdfcf::{gdalmd::MdGroup, Metadata, NetCdfCfDataProvider},
    util::config::get_config_element,
};
use gdal::{raster::RasterCreationOption, Dataset, DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::{
    error::BoxedResultExt, primitives::TimeInterval, util::gdal::ResamplingMethod,
};
use geoengine_operators::{
    source::{GdalLoadingInfoTemporalSlice, GdalMetaDataList, GdalMetadataNetCdfCf},
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

#[derive(Debug, Clone, Copy)]
pub enum OverviewGeneration {
    Created,
    Skipped,
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
    resampling_method: Option<ResamplingMethod>,
) -> Result<OverviewGeneration> {
    let file_path = provider_path.join(dataset_path);
    let out_folder_path = overview_path.join(dataset_path);

    if !out_folder_path.exists() {
        fs::create_dir_all(&out_folder_path).boxed_context(error::InvalidDirectory)?;
    }

    let in_progress_flag = InProgressFlag::create(&out_folder_path)?;

    // TODO: refactor with new method from Michael
    // check that file does not "escape" the provider path
    if let Err(source) = file_path.strip_prefix(provider_path) {
        return Err(NetCdfCf4DProviderError::DatasetIsNotInProviderPath { source });
    }
    // TODO: refactor with new method from Michael
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

    match store_metadata(provider_path, dataset_path, &out_folder_path) {
        Ok(OverviewGeneration::Created) => (),
        Ok(OverviewGeneration::Skipped) => return Ok(OverviewGeneration::Skipped),
        Err(e) => return Err(e),
    };

    let time_coverage = time_coverage(&root_group)?;

    for conversion in group_tree.conversion_metadata(&file_path, &out_folder_path) {
        match index_subdataset(&conversion, &time_coverage, resampling_method) {
            Ok(OverviewGeneration::Created) => (),
            Ok(OverviewGeneration::Skipped) => return Ok(OverviewGeneration::Skipped),
            Err(e) => return Err(e),
        }
    }

    in_progress_flag.remove()?;

    Ok(OverviewGeneration::Created)
}

/// A flag that indicates on-going process of an overview folder.
///
/// Cleans up the folder if dropped.
pub struct InProgressFlag {
    path: PathBuf,
}

impl InProgressFlag {
    const IN_PROGRESS_FLAG_NAME: &'static str = ".in_progress";

    fn create(folder: &Path) -> Result<Self> {
        if !folder.is_dir() {
            return Err(NetCdfCf4DProviderError::InvalidDirectory {
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound, // TODO: use `NotADirectory` if stable
                    folder.to_string_lossy().to_string(),
                )),
            });
        }
        let this = Self {
            path: folder.join(Self::IN_PROGRESS_FLAG_NAME),
        };

        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&this.path)
            .boxed_context(error::CannotCreateInProgressFlag)?;

        Ok(this)
    }

    fn remove(self) -> Result<()> {
        fs::remove_file(&self.path).boxed_context(error::CannotRemoveInProgressFlag)?;
        Ok(())
    }

    pub fn is_in_progress(folder: &Path) -> bool {
        if !folder.is_dir() {
            return false;
        }

        let path = folder.join(Self::IN_PROGRESS_FLAG_NAME);

        path.exists()
    }
}

impl Drop for InProgressFlag {
    fn drop(&mut self) {
        if !self.path.exists() {
            return;
        }

        if let Err(e) = fs::remove_file(&self.path).boxed_context(error::CannotRemoveInProgressFlag)
        {
            log::error!("Cannot remove in progress flag: {}", e);
        }
    }
}

fn store_metadata(
    provider_path: &Path,
    dataset_path: &Path,
    out_folder_path: &Path,
) -> Result<OverviewGeneration> {
    let out_file_path = out_folder_path.join(METADATA_FILE_NAME);

    if out_file_path.exists() {
        debug!("Skipping metadata generation: {}", dataset_path.display());
        return Ok(OverviewGeneration::Skipped);
    }

    debug!("Creating metadata: {}", dataset_path.display());

    let metadata =
        NetCdfCfDataProvider::build_netcdf_tree(provider_path, None, dataset_path, true)?;

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

    Ok(OverviewGeneration::Created)
}

fn index_subdataset(
    conversion: &ConversionMetadata,
    time_coverage: &TimeCoverage,
    resampling_method: Option<ResamplingMethod>,
) -> Result<OverviewGeneration> {
    const COG_BLOCK_SIZE: &str = "512";
    const COMPRESSION_FORMAT: &str = "LZW"; // this is the GDAL default
    const DEFAULT_COMPRESSION_LEVEL: u8 = 6; // this is the GDAL default
    const DEFAULT_RESAMPLING_METHOD: ResamplingMethod = ResamplingMethod::Nearest;

    if conversion.dataset_out.exists() {
        debug!("Skipping conversion: {}", conversion.dataset_out.display());
        return Ok(OverviewGeneration::Skipped);
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

    let gdal_options = get_config_element::<crate::util::config::Gdal>()
        .boxed_context(error::CannotCreateOverviews)?;
    let num_threads = gdal_options.compression_num_threads.to_string();
    let compression_format = gdal_options
        .compression_algorithm
        .as_deref()
        .unwrap_or(COMPRESSION_FORMAT);
    let compression_level = gdal_options
        .compression_z_level
        .unwrap_or(DEFAULT_COMPRESSION_LEVEL)
        .to_string();
    let resampling_method = resampling_method
        .unwrap_or(DEFAULT_RESAMPLING_METHOD)
        .to_string();

    let cog_driver = gdal::Driver::get("COG").boxed_context(error::CannotCreateOverviews)?;
    let options = vec![
        RasterCreationOption {
            key: "COMPRESS",
            value: compression_format,
        },
        RasterCreationOption {
            key: "LEVEL",
            value: compression_level.as_ref(),
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
        RasterCreationOption {
            key: "RESAMPLING",
            value: resampling_method.as_ref(),
        },
    ];

    debug!("Overview creation GDAL options: {:?}", &options);

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

    Ok(OverviewGeneration::Created)
}

fn generate_loading_info(
    dataset: &Dataset,
    overview_dataset_path: &Path,
    time_coverage: &TimeCoverage,
) -> Result<Metadata> {
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

    Ok(match *time_coverage {
        TimeCoverage::Regular { start, end, step } => {
            Metadata::NetCDF(GdalMetadataNetCdfCf {
                result_descriptor,
                params,
                start,
                end,
                step,
                band_offset: 0, // must be changed if there are other dimensions then geo & time
            })
        }
        TimeCoverage::List { ref time_stamps } => {
            let mut params_list = Vec::with_capacity(time_stamps.len());
            for (i, time_instance) in time_stamps.iter().enumerate() {
                let mut params = params.clone();

                params.rasterband_channel = i + 1;

                params_list.push(GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::new_instant(*time_instance)
                        .context(error::InvalidTimeCoverageInterval)?,
                    params: Some(params),
                });
            }

            Metadata::List(GdalMetaDataList {
                result_descriptor,
                params: params_list,
            })
        }
    })
}

pub fn remove_overviews(dataset_path: &Path, overview_path: &Path, force: bool) -> Result<()> {
    let out_folder_path = overview_path.join(dataset_path);

    // TODO: refactor with new method from Michael
    // check that file does not "escape" the overview path
    if let Err(source) = out_folder_path.strip_prefix(overview_path) {
        return Err(NetCdfCf4DProviderError::DatasetIsNotInProviderPath { source });
    }

    if !out_folder_path.exists() {
        return Ok(());
    }

    if !force && InProgressFlag::is_in_progress(&out_folder_path) {
        return Err(NetCdfCf4DProviderError::CannotRemoveOverviewsWhileCreationIsInProgress);
    }

    fs::remove_dir_all(&out_folder_path).boxed_context(error::CannotRemoveOverviews)?;

    // traverse up and remove folder if empty

    let mut dataset_path = dataset_path.to_path_buf();
    while dataset_path.pop() && /* don't delete `/` */ dataset_path.parent().is_some() {
        let partial_folder_path = overview_path.join(&dataset_path);

        if fs::remove_dir(partial_folder_path).is_err() {
            break; // stop traversing up if folder is not empty
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datasets::external::netcdfcf::{
        NetCdfEntity, NetCdfGroup as FullNetCdfGroup, NetCdfOverview,
    };
    use geoengine_datatypes::{
        operations::image::{Colorizer, RgbaColor},
        primitives::{DateTime, Measurement, TimeGranularity, TimeStep},
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
            &TimeCoverage::Regular {
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
            Metadata::NetCDF(GdalMetadataNetCdfCf {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
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
                    allow_alphaband_as_mask: true,
                },
                start: DateTime::new_utc(2020, 1, 1, 0, 0, 0).into(),
                end: DateTime::new_utc(2021, 1, 1, 0, 0, 0).into(),
                step: TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
                band_offset: 0,
            })
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
            &TimeCoverage::Regular {
                start: DateTime::new_utc(2020, 1, 1, 0, 0, 0).into(),
                end: DateTime::new_utc(2021, 1, 1, 0, 0, 0).into(),
                step: TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
            },
            None,
        )
        .unwrap();

        assert!(dataset_out.exists());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
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
                        data_range: Some((1., 97.)),
                        unit: "".to_string(),
                        groups: vec![]
                    },
                    FullNetCdfGroup {
                        name: "metric_2".to_string(),
                        title: "Random metric 2".to_string(),
                        description: "Randomly created data".to_string(),
                        data_type: Some(RasterDataType::I16),
                        data_range: Some((1., 98.)),
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
                time_coverage: TimeCoverage::Regular {
                    start: DateTime::new_utc(2000, 1, 1, 0, 0, 0).into(),
                    end: DateTime::new_utc(2003, 1, 1, 0, 0, 0).into(),
                    step: TimeStep {
                        granularity: TimeGranularity::Years,
                        step: 1
                    }
                },
                colorizer: Colorizer::LinearGradient {
                    breakpoints: vec![
                        (
                            0.0.try_into().expect("not nan"),
                            RgbaColor::new(68, 1, 84, 255),
                        )
                            .into(),
                        (
                            36.428_571_428_571_42.try_into().expect("not nan"),
                            RgbaColor::new(70, 50, 126, 255),
                        )
                            .into(),
                        (
                            72.857_142_857_142_85.try_into().expect("not nan"),
                            RgbaColor::new(54, 92, 141, 255),
                        )
                            .into(),
                        (
                            109.285_714_285_714_28.try_into().expect("not nan"),
                            RgbaColor::new(39, 127, 142, 255),
                        )
                            .into(),
                        (
                            109.285_714_285_714_28.try_into().expect("not nan"),
                            RgbaColor::new(31, 161, 135, 255),
                        )
                            .into(),
                        (
                            182.142_857_142_857_1.try_into().expect("not nan"),
                            RgbaColor::new(74, 193, 109, 255),
                        )
                            .into(),
                        (
                            218.571_428_571_428_53.try_into().expect("not nan"),
                            RgbaColor::new(160, 218, 57, 255),
                        )
                            .into(),
                        (
                            255.0.try_into().expect("not nan"),
                            RgbaColor::new(253, 231, 37, 255),
                        )
                            .into(),
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
            None,
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

    #[test]
    fn test_remove_overviews() {
        fn is_empty(directory: &Path) -> bool {
            directory.read_dir().unwrap().next().is_none()
        }

        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let dataset_path = Path::new("dataset_m.nc");

        create_overviews(
            test_data!("netcdf4d"),
            dataset_path,
            overview_folder.path(),
            None,
        )
        .unwrap();

        assert!(!is_empty(overview_folder.path()));

        remove_overviews(dataset_path, overview_folder.path(), false).unwrap();

        assert!(is_empty(overview_folder.path()));
    }
}
