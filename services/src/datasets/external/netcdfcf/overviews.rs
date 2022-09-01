use super::{error, NetCdfCf4DProviderError, TimeCoverage};
use crate::{
    datasets::{external::netcdfcf::NetCdfCfDataProvider, storage::MetaDataDefinition},
    tasks::{TaskContext, TaskStatusInfo},
    util::{canonicalize_subpath, config::get_config_element, path_with_base_path},
};
use gdal::{
    cpl::CslStringList,
    programs::raster::{
        multi_dim_translate, MultiDimTranslateDestination, MultiDimTranslateOptions,
    },
    raster::{Group, RasterBand, RasterCreationOption},
    Dataset, DatasetOptions, GdalOpenFlags,
};
use gdal_sys::GDALGetRasterStatistics;
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{DateTimeParseFormat, TimeInstance, TimeInterval},
    util::gdal::ResamplingMethod,
};
use geoengine_operators::{
    source::{
        GdalLoadingInfoTemporalSlice, GdalMetaDataList, GdalMetaDataRegular,
        GdalSourceTimePlaceholder, TimeReference,
    },
    util::gdal::{
        gdal_open_dataset_ex, gdal_parameters_from_dataset, raster_descriptor_from_dataset,
    },
};
use log::debug;
use snafu::ResultExt;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

pub const METADATA_FILE_NAME: &str = "metadata.json";
pub const LOADING_INFO_FILE_NAME: &str = "loading_info.json";

#[derive(Debug, Clone)]
struct NetCdfGroup {
    name: String,
    groups: Vec<NetCdfGroup>,
    arrays: Vec<NetCdfArray>,
}

#[derive(Debug, Clone)]
struct NetCdfArray {
    name: String,
    pub number_of_entities: usize,
}

#[derive(Debug, Clone)]
struct ConversionMetadata {
    pub dataset_in: String,
    pub dataset_out_base: PathBuf,
    pub array_path: String,
    pub number_of_entities: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum OverviewGeneration {
    Created,
    Skipped,
}

impl NetCdfGroup {
    fn flatten(&self) -> Vec<(Vec<String>, NetCdfArray)> {
        let mut out_paths = Vec::new();

        for group in &self.groups {
            out_paths.extend(group.flatten_mut(Vec::new()));
        }

        for array in &self.arrays {
            out_paths.push((vec![], array.clone()));
        }

        out_paths
    }

    fn flatten_mut(&self, mut path_vec: Vec<String>) -> Vec<(Vec<String>, NetCdfArray)> {
        let mut out_paths = Vec::new();

        path_vec.push(self.name.clone());

        for group in &self.groups {
            out_paths.extend(group.flatten_mut(path_vec.clone()));
        }

        for array in &self.arrays {
            out_paths.push((path_vec.clone(), array.clone()));
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

        for (mut data_path, array) in self.flatten() {
            let dataset_out_base = out_root_path.join(data_path.join("/"));

            data_path.push(array.name);
            let array_path = data_path.join("/");

            metadata.push(ConversionMetadata {
                dataset_in: format!("NETCDF:\"{in_path}\""),
                dataset_out_base,
                array_path,
                number_of_entities: array.number_of_entities,
            });
        }

        metadata
    }
}

trait NetCdfVisitor {
    fn group_tree(&self) -> Result<NetCdfGroup>;

    fn array_names_options() -> CslStringList {
        let mut options = CslStringList::new();
        options
            .set_name_value("SHOW_ZERO_DIM", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("SHOW_COORDINATES", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("SHOW_INDEXING", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("SHOW_BOUNDS", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("SHOW_TIME", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("GROUP_BY", "SAME_DIMENSION")
            .unwrap_or_else(|e| debug!("{}", e));
        options
    }
}

impl NetCdfVisitor for Group<'_> {
    fn group_tree(&self) -> Result<NetCdfGroup> {
        let mut groups = Vec::new();
        for subgroup_name in self.group_names(Default::default()) {
            let subgroup = self
                .open_group(&subgroup_name, Default::default())
                .context(error::GdalMd)?;
            groups.push(subgroup.group_tree()?);
        }

        let dimension_names: HashSet<String> = self
            .dimensions(Self::array_names_options())
            .map_err(|source| NetCdfCf4DProviderError::CannotReadDimensions { source })?
            .into_iter()
            .map(|dim| dim.name())
            .collect();

        let mut arrays = Vec::new();
        for array_name in self.array_names(Self::array_names_options()) {
            // filter out arrays that are actually dimensions
            if dimension_names.contains(&array_name) {
                continue;
            }

            let md_array = self
                .open_md_array(&array_name, Default::default())
                .context(error::GdalMd)?;

            let mut number_of_entities = 0;

            for dimension in md_array.dimensions().context(error::GdalMd)? {
                if &dimension.name() == "entity" {
                    number_of_entities = dimension.size();
                }
            }

            arrays.push(NetCdfArray {
                name: array_name.to_string(),
                number_of_entities,
            });
        }

        Ok(NetCdfGroup {
            name: self.name(),
            groups,
            arrays,
        })
    }
}

pub fn create_overviews<C: TaskContext>(
    provider_path: &Path,
    dataset_path: &Path,
    overview_path: &Path,
    resampling_method: Option<ResamplingMethod>,
    task_context: &C,
) -> Result<OverviewGeneration> {
    let file_path = canonicalize_subpath(provider_path, dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;
    let out_folder_path = path_with_base_path(overview_path, dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;

    let dataset = gdal_open_dataset_ex(
        &file_path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY | GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
            allowed_drivers: Some(&["netCDF"]),
            open_options: None,
            sibling_files: None,
        },
    )
    .boxed_context(error::CannotOpenNetCdfDataset)?;

    let root_group = dataset.root_group().context(error::GdalMd)?;
    let group_tree = root_group.group_tree()?;
    let time_coverage = TimeCoverage::from_root_group(&root_group)?;

    if !out_folder_path.exists() {
        fs::create_dir_all(&out_folder_path).boxed_context(error::InvalidDirectory)?;
    }

    // must have this flag before any write operations
    let in_progress_flag = InProgressFlag::create(&out_folder_path)?;

    let conversion_metadata = group_tree.conversion_metadata(&file_path, &out_folder_path);
    let number_of_conversions = conversion_metadata.len();

    let mut stats_for_group = HashMap::<String, (f64, f64)>::new();

    for (i, conversion) in conversion_metadata.into_iter().enumerate() {
        match index_subdataset(
            &conversion,
            &time_coverage,
            resampling_method,
            task_context,
            &mut stats_for_group,
            i,
            number_of_conversions,
        ) {
            Ok(OverviewGeneration::Created) => (),
            Ok(OverviewGeneration::Skipped) => return Ok(OverviewGeneration::Skipped),
            Err(e) => return Err(e),
        }
    }

    emit_status(
        task_context,
        0.9, // just say the last 10% are metadata
        "Collecting metadata".to_string(),
    );

    match store_metadata(
        provider_path,
        dataset_path,
        &out_folder_path,
        &stats_for_group,
    ) {
        Ok(OverviewGeneration::Created) => (),
        Ok(OverviewGeneration::Skipped) => return Ok(OverviewGeneration::Skipped),
        Err(e) => return Err(e),
    };

    in_progress_flag.remove()?;

    Ok(OverviewGeneration::Created)
}

fn emit_status<C: TaskContext>(task_context: &C, pct: f64, status: String) {
    // TODO: more elegant way to do this?
    tokio::task::block_in_place(move || {
        tokio::runtime::Handle::current().block_on(async move {
            task_context.set_completion(pct, status.boxed()).await;
        });
    });
}

fn emit_subtask_status<C: TaskContext>(
    conversion_index: usize,
    number_of_conversions: usize,
    entity: u32,
    number_of_other_entities: u32,
    task_context: &C,
) {
    let min_pct = conversion_index as f64 / number_of_conversions as f64;
    let max_pct = (conversion_index + 1) as f64 / number_of_conversions as f64;
    let dimension_pct = f64::from(entity) / f64::from(number_of_other_entities);
    emit_status(
        task_context,
        min_pct + dimension_pct * (max_pct - min_pct),
        format!("Processing {} of {number_of_conversions} subdatasets; Entity {entity} of {number_of_other_entities}", conversion_index + 1),
    );
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
    stats_for_group: &HashMap<String, (f64, f64)>,
) -> Result<OverviewGeneration> {
    let out_file_path = out_folder_path.join(METADATA_FILE_NAME);

    if out_file_path.exists() {
        debug!("Skipping metadata generation: {}", dataset_path.display());
        return Ok(OverviewGeneration::Skipped);
    }

    debug!("Creating metadata: {}", dataset_path.display());

    let metadata = NetCdfCfDataProvider::build_netcdf_tree(
        provider_path,
        None,
        dataset_path,
        stats_for_group,
    )?;

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

fn index_subdataset<C: TaskContext>(
    conversion: &ConversionMetadata,
    time_coverage: &TimeCoverage,
    resampling_method: Option<ResamplingMethod>,
    task_context: &C,
    stats_for_group: &mut HashMap<String, (f64, f64)>,
    conversion_index: usize,
    number_of_conversions: usize,
) -> Result<OverviewGeneration> {
    if conversion.dataset_out_base.exists() {
        debug!(
            "Skipping conversion: {}",
            conversion.dataset_out_base.display()
        );
        return Ok(OverviewGeneration::Skipped);
    }

    debug!(
        "Indexing conversion: {}",
        conversion.dataset_out_base.display()
    );

    let subdataset = gdal_open_dataset_ex(
        Path::new(&conversion.dataset_in),
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY | GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
            allowed_drivers: Some(&["netCDF"]),
            open_options: None,
            sibling_files: None,
        },
    )
    .boxed_context(error::CannotOpenNetCdfSubdataset)?;

    let raster_creation_options = CogRasterCreationOptions::new(resampling_method)?;
    let raster_creation_options = raster_creation_options.options();

    debug!(
        "Overview creation GDAL options: {:?}",
        &raster_creation_options
    );

    let time_steps = time_coverage.time_steps()?;

    let (mut value_min, mut value_max) = (f64::INFINITY, -f64::INFINITY);

    for entity in 0..conversion.number_of_entities {
        emit_subtask_status(
            conversion_index,
            number_of_conversions,
            entity as u32,
            conversion.number_of_entities as u32,
            task_context,
        );

        let entity_directory = conversion.dataset_out_base.join(entity.to_string());

        fs::create_dir_all(entity_directory).boxed_context(error::CannotCreateOverviews)?;

        let mut first_overview_dataset = None;

        for (time_idx, time_step) in time_steps.iter().enumerate() {
            let CreateSubdatasetTiffResult {
                overview_dataset,
                overview_destination,
                min_max,
            } = create_subdataset_tiff(
                *time_step,
                conversion,
                entity,
                &raster_creation_options,
                &subdataset,
                time_idx,
            )?;

            if let Some((min, max)) = min_max {
                value_min = value_min.min(min);
                value_max = value_max.max(max);
            }
            if time_idx == 0 {
                first_overview_dataset = Some((overview_dataset, overview_destination));
            }
        }

        let (overview_dataset, overview_destination) = match first_overview_dataset {
            Some(overview_dataset) => overview_dataset,
            None => {
                return Err(NetCdfCf4DProviderError::NoOverviewsGeneratedForSource {
                    path: conversion.dataset_out_base.to_string_lossy().to_string(),
                });
            }
        };

        let loading_info =
            generate_loading_info(&overview_dataset, &overview_destination, time_coverage)?;

        let loading_info_file =
            File::create(overview_destination.with_file_name(LOADING_INFO_FILE_NAME))
                .boxed_context(error::CannotWriteMetadataFile)?;

        let mut writer = BufWriter::new(loading_info_file);

        writer
            .write_all(
                serde_json::to_string(&loading_info)
                    .boxed_context(error::CannotWriteMetadataFile)?
                    .as_bytes(),
            )
            .boxed_context(error::CannotWriteMetadataFile)?;

        // remove array from path and insert to `stats_for_group`
        if let Some((array_path_stripped, _)) = conversion.array_path.rsplit_once('/') {
            stats_for_group.insert(array_path_stripped.to_string(), (value_min, value_max));
        }
    }

    Ok(OverviewGeneration::Created)
}

struct CreateSubdatasetTiffResult {
    overview_dataset: Dataset,
    overview_destination: PathBuf,
    min_max: Option<(f64, f64)>,
}

fn create_subdataset_tiff(
    time_step: TimeInstance,
    conversion: &ConversionMetadata,
    entity: usize,
    raster_creation_options: &Vec<RasterCreationOption>,
    subdataset: &Dataset,
    time_idx: usize,
) -> Result<CreateSubdatasetTiffResult> {
    let time_str = time_step.as_rfc3339_with_millis();
    let destination = conversion
        .dataset_out_base
        .join(entity.to_string())
        .join(time_str + ".tiff");
    let name = format!("/{}", conversion.array_path);
    let view = format!("[{entity},{time_idx},:,:]",);
    let mut options = vec![
        "-array".to_string(),
        format!("name={name},view={view}"),
        "-of".to_string(),
        "COG".to_string(),
    ];
    for raster_creation_option in raster_creation_options {
        options.push("-co".to_string());
        options.push(format!(
            "{key}={value}",
            key = raster_creation_option.key,
            value = raster_creation_option.value
        ));
    }
    let overview_dataset = multi_dim_translate(
        &[subdataset],
        MultiDimTranslateDestination::path(&destination).context(error::GdalMd)?,
        Some(MultiDimTranslateOptions::new(options).context(error::GdalMd)?),
    )
    .context(error::GdalMd)?;
    let min_max = (|| unsafe {
        let c_band =
            gdal_sys::GDALGetRasterBand(overview_dataset.c_dataset(), 1 as std::ffi::c_int);
        if c_band.is_null() {
            return None;
        }

        let mut min = 0.;
        let mut max = 0.;
        let rv = GDALGetRasterStatistics(
            c_band,
            std::ffi::c_int::from(false),
            std::ffi::c_int::from(true),
            std::ptr::addr_of_mut!(min),
            std::ptr::addr_of_mut!(max),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );

        RasterBand::from_c_rasterband(&overview_dataset, c_band);

        if rv != gdal_sys::CPLErr::CE_None {
            return None;
        }

        Some((min, max))
    })();

    Ok(CreateSubdatasetTiffResult {
        overview_dataset,
        overview_destination: destination,
        min_max,
    })
}

struct CogRasterCreationOptions {
    compression_format: String,
    compression_level: String,
    num_threads: String,
    resampling_method: String,
}

impl CogRasterCreationOptions {
    fn new(resampling_method: Option<ResamplingMethod>) -> Result<Self> {
        const COMPRESSION_FORMAT: &str = "LZW"; // this is the GDAL default
        const DEFAULT_COMPRESSION_LEVEL: u8 = 6; // this is the GDAL default
        const DEFAULT_RESAMPLING_METHOD: ResamplingMethod = ResamplingMethod::Nearest;

        let gdal_options = get_config_element::<crate::util::config::Gdal>()
            .boxed_context(error::CannotCreateOverviews)?;
        let num_threads = gdal_options.compression_num_threads.to_string();
        let compression_format = gdal_options
            .compression_algorithm
            .as_deref()
            .unwrap_or(COMPRESSION_FORMAT)
            .to_string();
        let compression_level = gdal_options
            .compression_z_level
            .unwrap_or(DEFAULT_COMPRESSION_LEVEL)
            .to_string();
        let resampling_method = resampling_method
            .unwrap_or(DEFAULT_RESAMPLING_METHOD)
            .to_string();

        Ok(Self {
            compression_format,
            compression_level,
            num_threads,
            resampling_method,
        })
    }
}

impl CogRasterCreationOptions {
    fn options(&self) -> Vec<RasterCreationOption<'_>> {
        const COG_BLOCK_SIZE: &str = "512";

        vec![
            RasterCreationOption {
                key: "COMPRESS",
                value: &self.compression_format,
            },
            RasterCreationOption {
                key: "LEVEL",
                value: &self.compression_level,
            },
            RasterCreationOption {
                key: "NUM_THREADS",
                value: &self.num_threads,
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
                value: &self.resampling_method,
            },
        ]
    }
}

fn generate_loading_info(
    dataset: &Dataset,
    overview_dataset_path: &Path,
    time_coverage: &TimeCoverage,
) -> Result<MetaDataDefinition> {
    const ONLY_BAND: usize = 1;

    let result_descriptor = raster_descriptor_from_dataset(dataset, 1)
        .boxed_context(error::CannotGenerateLoadingInfo)?;

    let mut params = gdal_parameters_from_dataset(
        dataset,
        ONLY_BAND,
        overview_dataset_path,
        Some(ONLY_BAND),
        None,
    )
    .boxed_context(error::CannotGenerateLoadingInfo)?;

    Ok(match *time_coverage {
        TimeCoverage::Regular { start, end, step } => {
            let time_interval =
                TimeInterval::new(start, end).context(error::InvalidTimeCoverageInterval)?;

            let placeholder = "%_START_TIME_%".to_string();
            params.file_path = params
                .file_path
                .with_file_name(placeholder.clone() + ".tiff");

            MetaDataDefinition::GdalMetaDataRegular(GdalMetaDataRegular {
                result_descriptor,
                params,
                step,
                time_placeholders: [(
                    placeholder,
                    GdalSourceTimePlaceholder {
                        format: DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%.3fZ".to_string()),
                        reference: TimeReference::Start,
                    },
                )]
                .into_iter()
                .collect(),
                data_time: time_interval,
            })
        }
        TimeCoverage::List { ref time_stamps } => {
            let mut params_list = Vec::with_capacity(time_stamps.len());
            for time_instance in time_stamps.iter() {
                let mut params = params.clone();

                params.file_path = params
                    .file_path
                    .with_file_name(time_instance.as_rfc3339_with_millis() + ".tiff");

                let time_interval = TimeInterval::new_instant(*time_instance)
                    .context(error::InvalidTimeCoverageInterval)?;

                params_list.push(GdalLoadingInfoTemporalSlice {
                    time: time_interval,
                    params: Some(params),
                });
            }

            MetaDataDefinition::GdalMetaDataList(GdalMetaDataList {
                result_descriptor,
                params: params_list,
            })
        }
    })
}

pub fn remove_overviews(dataset_path: &Path, overview_path: &Path, force: bool) -> Result<()> {
    let out_folder_path = path_with_base_path(overview_path, dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;

    if !out_folder_path.exists() {
        return Ok(());
    }

    if !force && InProgressFlag::is_in_progress(&out_folder_path) {
        return Err(NetCdfCf4DProviderError::CannotRemoveOverviewsWhileCreationIsInProgress);
    }

    fs::remove_dir_all(&out_folder_path).boxed_context(error::CannotRemoveOverviews)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        datasets::{
            external::netcdfcf::{NetCdfEntity, NetCdfGroup as FullNetCdfGroup, NetCdfOverview},
            storage::MetaDataDefinition,
        },
        tasks::util::NopTaskContext,
    };
    use geoengine_datatypes::{
        hashmap,
        operations::image::{Colorizer, RgbaColor},
        primitives::{DateTime, Measurement, TimeGranularity, TimeStep},
        raster::RasterDataType,
        spatial_reference::SpatialReference,
        test_data,
        util::gdal::hide_gdal_errors,
    };
    use geoengine_operators::{
        engine::RasterResultDescriptor,
        source::{
            FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
            GdalMetaDataRegular,
        },
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
            MetaDataDefinition::GdalMetaDataRegular(GdalMetaDataRegular {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                },
                params: GdalDatasetParameters {
                    file_path: Path::new("foo/%_START_TIME_%.tiff").into(),
                    rasterband_channel: 1,
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
                step: TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
                time_placeholders: hashmap! {
                    "%_START_TIME_%".to_string() => GdalSourceTimePlaceholder {
                        format: DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%.3fZ".to_string()),
                        reference: TimeReference::Start,
                    },
                },
                data_time: TimeInterval::new(
                    DateTime::new_utc(2020, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2021, 1, 1, 0, 0, 0)
                )
                .unwrap(),
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_index_subdataset() {
        hide_gdal_errors();

        let dataset_in = format!(
            "NETCDF:\"{}\"",
            test_data!("netcdf4d/dataset_m.nc").display()
        );

        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.path().join("metric_1");

        index_subdataset(
            &ConversionMetadata {
                dataset_in,
                dataset_out_base: tempdir_path.clone(),
                array_path: "/metric_1/ebv_cube".to_string(),
                number_of_entities: 3,
            },
            &TimeCoverage::Regular {
                start: DateTime::new_utc(2000, 1, 1, 0, 0, 0).into(),
                end: DateTime::new_utc(2003, 1, 1, 0, 0, 0).into(),
                step: TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1,
                },
            },
            None,
            &NopTaskContext,
            &mut Default::default(),
            0,
            1,
        )
        .unwrap();

        for entity in 0..3 {
            for year in 2000..=2002 {
                let path = tempdir_path.join(format!("{entity}/{year}-01-01T00:00:00.000Z.tiff"));
                assert!(path.exists(), "Path {} does not exist", path.display());
            }

            let path = tempdir_path.join(format!("{entity}/loading_info.json"));
            assert!(path.exists(), "Path {} does not exist", path.display());
        }

        let sample_loading_info =
            std::fs::read_to_string(tempdir_path.join("1/loading_info.json")).unwrap();
        assert_eq!(
            serde_json::from_str::<MetaDataDefinition>(&sample_loading_info).unwrap(),
            MetaDataDefinition::GdalMetaDataRegular(GdalMetaDataRegular {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                },
                params: GdalDatasetParameters {
                    file_path: tempdir_path.join("1/%_START_TIME_%.tiff"),
                    rasterband_channel: 1,
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
                step: TimeStep {
                    granularity: TimeGranularity::Years,
                    step: 1,
                },
                time_placeholders: hashmap! {
                    "%_START_TIME_%".to_string() => GdalSourceTimePlaceholder {
                        format: DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%.3fZ".to_string()),
                        reference: TimeReference::Start,
                    },
                },
                data_time: TimeInterval::new(
                    DateTime::new_utc(2000, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2003, 1, 1, 0, 0, 0)
                )
                .unwrap(),
            })
        );
    }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    // async fn test_index_subdataset_irregular() {
    //     hide_gdal_errors();

    //     let dataset_in = format!(
    //         "NETCDF:\"{}\"",
    //         test_data!("netcdf4d/dataset_irr_ts.nc").display()
    //     );

    //     let tempdir = tempfile::tempdir().unwrap();
    //     let tempdir_path = tempdir.path().join("metric_1");

    //     index_subdataset(
    //         &ConversionMetadata {
    //             dataset_in,
    //             dataset_out_base: tempdir_path.clone(),
    //             array_path: "/metric_1/ebv_cube".to_string(),
    //             number_of_entities: 3,
    //         },
    //         &&TimeCoverage::List { time_stamps: () } {
    //             start: DateTime::new_utc(2000, 1, 1, 0, 0, 0).into(),
    //             end: DateTime::new_utc(2003, 1, 1, 0, 0, 0).into(),
    //             step: TimeStep {
    //                 granularity: TimeGranularity::Years,
    //                 step: 1,
    //             },
    //         },
    //         None,
    //         &NopTaskContext,
    //         &mut Default::default(),
    //         0,
    //         1,
    //     )
    //     .unwrap();

    //     for entity in 0..3 {
    //         for year in 2000..=2002 {
    //             let path = tempdir_path.join(format!("{entity}/{year}-01-01T00:00:00.000Z.tiff"));
    //             assert!(path.exists(), "Path {} does not exist", path.display());
    //         }

    //         let path = tempdir_path.join(format!("{entity}/loading_info.json"));
    //         assert!(path.exists(), "Path {} does not exist", path.display());
    //     }

    //     let sample_loading_info =
    //         std::fs::read_to_string(tempdir_path.join("1/loading_info.json")).unwrap();
    //     assert_eq!(
    //         serde_json::from_str::<MetaDataDefinition>(&sample_loading_info).unwrap(),
    //         MetaDataDefinition::GdalMetaDataRegular(GdalMetaDataRegular {
    //             result_descriptor: RasterResultDescriptor {
    //                 data_type: RasterDataType::I16,
    //                 spatial_reference: SpatialReference::epsg_4326().into(),
    //                 measurement: Measurement::Unitless,
    //                 time: None,
    //                 bbox: None,
    //             },
    //             params: GdalDatasetParameters {
    //                 file_path: tempdir_path.join("1/%_START_TIME_%.tiff"),
    //                 rasterband_channel: 1,
    //                 geo_transform: GdalDatasetGeoTransform {
    //                     origin_coordinate: (50., 55.).into(),
    //                     x_pixel_size: 1.,
    //                     y_pixel_size: -1.,
    //                 },
    //                 width: 5,
    //                 height: 5,
    //                 file_not_found_handling: FileNotFoundHandling::Error,
    //                 no_data_value: Some(-9999.0),
    //                 properties_mapping: None,
    //                 gdal_open_options: None,
    //                 gdal_config_options: None,
    //                 allow_alphaband_as_mask: true,
    //             },
    //             step: TimeStep {
    //                 granularity: TimeGranularity::Years,
    //                 step: 1,
    //             },
    //             time_placeholders: hashmap! {
    //                 "%_START_TIME_%".to_string() => GdalSourceTimePlaceholder {
    //                     format: DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%.3fZ".to_string()),
    //                     reference: TimeReference::Start,
    //                 },
    //             },
    //             data_time: TimeInterval::new(
    //                 DateTime::new_utc(2000, 1, 1, 0, 0, 0),
    //                 DateTime::new_utc(2003, 1, 1, 0, 0, 0)
    //             )
    //             .unwrap(),
    //         })
    //     );
    // }

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
            &[
                ("metric_1".to_string(), (1., 97.)),
                ("metric_2".to_string(), (1., 98.)),
            ]
            .into_iter()
            .collect(),
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
                creator_name: Some("Luise Quoß".to_string()),
                creator_email: Some("luise.quoss@idiv.de".to_string()),
                creator_institution: Some(
                    "German Centre for Integrative Biodiversity Research (iDiv)".to_string()
                ),
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_create_overviews() {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        create_overviews(
            test_data!("netcdf4d"),
            Path::new("dataset_m.nc"),
            overview_folder.path(),
            None,
            &NopTaskContext,
        )
        .unwrap();

        let dataset_folder = overview_folder.path().join("dataset_m.nc");

        assert!(dataset_folder.is_dir());

        assert!(dataset_folder.join("metadata.json").exists());

        for metric in ["metric_1", "metric_2"] {
            for entity in 0..3 {
                assert!(dataset_folder
                    .join(format!("{metric}/{entity}/2000-01-01T00:00:00.000Z.tiff"))
                    .exists());
                assert!(dataset_folder
                    .join(format!("{metric}/{entity}/2001-01-01T00:00:00.000Z.tiff"))
                    .exists());
                assert!(dataset_folder
                    .join(format!("{metric}/{entity}/2002-01-01T00:00:00.000Z.tiff"))
                    .exists());

                assert!(dataset_folder
                    .join(format!("{metric}/{entity}/loading_info.json"))
                    .exists());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn test_create_overviews_irregular() {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        create_overviews(
            test_data!("netcdf4d"),
            Path::new("dataset_irr_ts.nc"),
            overview_folder.path(),
            None,
            &NopTaskContext,
        )
        .unwrap();

        let dataset_folder = overview_folder.path().join("dataset_irr_ts.nc");

        assert!(dataset_folder.is_dir());

        assert!(dataset_folder.join("metadata.json").exists());

        for metric in ["metric_1", "metric_2"] {
            for entity in 0..3 {
                assert!(dataset_folder
                    .join(format!("{metric}/{entity}/1900-01-01T00:00:00.000Z.tiff"))
                    .exists());
                assert!(dataset_folder
                    .join(format!("{metric}/{entity}/2015-01-01T00:00:00.000Z.tiff"))
                    .exists());
                assert!(dataset_folder
                    .join(format!("{metric}/{entity}/2055-01-01T00:00:00.000Z.tiff"))
                    .exists());

                assert!(dataset_folder
                    .join(format!("{metric}/{entity}/loading_info.json"))
                    .exists());
            }
        }

        let sample_loading_info =
            std::fs::read_to_string(dataset_folder.join("metric_2/0/loading_info.json")).unwrap();
        assert_eq!(
            serde_json::from_str::<MetaDataDefinition>(&sample_loading_info).unwrap(),
            MetaDataDefinition::GdalMetaDataList(GdalMetaDataList {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                },
                params: vec![
                    GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new(
                            DateTime::new_utc(1900, 1, 1, 0, 0, 0),
                            DateTime::new_utc(1900, 1, 1, 0, 0, 0)
                        )
                        .unwrap(),
                        params: Some(GdalDatasetParameters {
                            file_path: dataset_folder
                                .join("metric_2/0/1900-01-01T00:00:00.000Z.tiff"),
                            rasterband_channel: 1,
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
                        }),
                    },
                    GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new(
                            DateTime::new_utc(2015, 1, 1, 0, 0, 0),
                            DateTime::new_utc(2015, 1, 1, 0, 0, 0)
                        )
                        .unwrap(),
                        params: Some(GdalDatasetParameters {
                            file_path: dataset_folder
                                .join("metric_2/0/2015-01-01T00:00:00.000Z.tiff"),
                            rasterband_channel: 1,
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
                        }),
                    },
                    GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new(
                            DateTime::new_utc(2055, 1, 1, 0, 0, 0),
                            DateTime::new_utc(2055, 1, 1, 0, 0, 0)
                        )
                        .unwrap(),
                        params: Some(GdalDatasetParameters {
                            file_path: dataset_folder
                                .join("metric_2/0/2055-01-01T00:00:00.000Z.tiff"),
                            rasterband_channel: 1,
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
                        }),
                    }
                ],
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_remove_overviews() {
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
            &NopTaskContext,
        )
        .unwrap();

        assert!(!is_empty(overview_folder.path()));

        remove_overviews(dataset_path, overview_folder.path(), false).unwrap();

        assert!(is_empty(overview_folder.path()));
    }
}
