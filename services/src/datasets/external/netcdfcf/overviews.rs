use super::{error, NetCdfCf4DProviderError, TimeCoverage};
use crate::{
    datasets::{
        external::netcdfcf::{gdalmd::MdGroup, NetCdfCfDataProvider},
        storage::MetaDataDefinition,
    },
    tasks::{TaskContext, TaskStatusInfo},
    util::config::get_config_element,
};
use gdal::{raster::RasterCreationOption, Dataset, DatasetOptions, GdalOpenFlags};
use gdal_sys::VSIUnlink;
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{DateTimeParseFormat, TimeInterval},
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
use snafu::{OptionExt, ResultExt};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufWriter, Write},
    ops::Deref,
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
    pub dataset_out_folder: PathBuf,
    pub number_of_time_steps: u32,
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
        number_of_time_steps: u32,
    ) -> Vec<ConversionMetadata> {
        let in_path = file_path.to_string_lossy();
        let mut metadata = Vec::new();

        for data_path in self.flatten() {
            let md_path = data_path.join("/");
            metadata.push(ConversionMetadata {
                dataset_in: format!("NETCDF:\"{in_path}\":/{md_path}"),
                dataset_out_folder: out_root_path.join(md_path),
                number_of_time_steps,
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

pub fn create_overviews<C: TaskContext>(
    provider_path: &Path,
    dataset_path: &Path,
    overview_path: &Path,
    resampling_method: Option<ResamplingMethod>,
    task_context: &C,
) -> Result<OverviewGeneration> {
    let file_path = provider_path.join(dataset_path);
    let out_folder_path = overview_path.join(dataset_path);

    check_paths(&file_path, provider_path, &out_folder_path, overview_path)?;

    let (group_tree, time_coverage) = create_group_tree_and_time_coverage(&file_path)?;

    if !out_folder_path.exists() {
        fs::create_dir_all(&out_folder_path).boxed_context(error::InvalidDirectory)?;
    }

    // must have this flag before any write operations
    let in_progress_flag = InProgressFlag::create(&out_folder_path)?;

    // TODO: defer calculating stats
    match store_metadata(provider_path, dataset_path, &out_folder_path) {
        Ok(OverviewGeneration::Created) => (),
        Ok(OverviewGeneration::Skipped) => return Ok(OverviewGeneration::Skipped),
        Err(e) => return Err(e),
    };

    let conversion_metadata = group_tree.conversion_metadata(
        &file_path,
        &out_folder_path,
        time_coverage.number_of_time_steps()?,
    );
    let number_of_conversions = conversion_metadata.len();
    for (i, conversion) in conversion_metadata.into_iter().enumerate() {
        emit_status(
            task_context,
            i as f64 / number_of_conversions as f64,
            format!(
                "Processing {} of {number_of_conversions} subdatasets",
                i + 1
            ),
        );

        match index_subdataset(
            &conversion,
            &time_coverage,
            resampling_method,
            task_context,
            i,
            number_of_conversions,
        ) {
            Ok(OverviewGeneration::Created) => (),
            Ok(OverviewGeneration::Skipped) => return Ok(OverviewGeneration::Skipped),
            Err(e) => return Err(e),
        };
    }

    in_progress_flag.remove()?;

    Ok(OverviewGeneration::Created)
}

fn emit_status<C: TaskContext>(task_context: &C, pct: f64, status: String) {
    // TODO: more elegant way to do this?
    tokio::task::block_in_place(move || {
        tokio::runtime::Handle::current().block_on(async move {
            task_context
                .set_completion((pct * 100.) as u8, status.boxed())
                .await;
        });
    });
}

fn check_paths(
    file_path: &Path,
    provider_path: &Path,
    out_folder_path: &Path,
    overview_path: &Path,
) -> Result<()> {
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
    Ok(())
}

fn create_group_tree_and_time_coverage(file_path: &Path) -> Result<(NetCdfGroup, TimeCoverage)> {
    let dataset = gdal_open_dataset_ex(
        file_path,
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

    let time_coverage = TimeCoverage::from_root_group(&root_group)?;

    Ok((group_tree, time_coverage))
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

#[derive(Debug)]
struct TempVrt {
    mem_path: String,
    dataset: Dataset,
}

impl TempVrt {
    fn new(dataset_in: &Dataset, band: u32) -> Result<Self> {
        let mem_path = format!("/vsimem/{}", uuid::Uuid::new_v4());

        // Get raw handles to the datasets
        let mut datasets_raw: Vec<gdal_sys::GDALDatasetH> = vec![unsafe { dataset_in.c_dataset() }];

        let c_dest = std::ffi::CString::new(mem_path.as_bytes()).context(error::CFfi)?;

        let cstr_args = vec!["-b".to_string(), band.to_string()]
            .into_iter()
            .map(|v| std::ffi::CString::new(v).context(error::CFfi))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut c_args = cstr_args
            .iter()
            .map(|x| x.as_ptr() as *mut std::ffi::c_char)
            .chain(std::iter::once(std::ptr::null_mut()))
            .collect::<Vec<_>>();
        let c_options =
            unsafe { gdal_sys::GDALBuildVRTOptionsNew(c_args.as_mut_ptr(), std::ptr::null_mut()) };

        let dataset_out = unsafe {
            gdal_sys::GDALBuildVRT(
                c_dest.as_ptr(),
                datasets_raw.len() as std::ffi::c_int,
                datasets_raw.as_mut_ptr(),
                std::ptr::null(),
                c_options,
                std::ptr::null_mut(),
            )
        };

        unsafe {
            gdal_sys::GDALBuildVRTOptionsFree(c_options);
        }

        if dataset_out.is_null() {
            return Err(unsafe { super::gdalmd::MdGroup::_last_null_pointer_err("GDALBuildVRT") })
                .context(error::CannotCreateVrt);
        }

        let dataset = unsafe { Dataset::from_c_dataset(dataset_out) };

        Ok(Self { mem_path, dataset })
    }
}

impl Deref for TempVrt {
    type Target = Dataset;

    fn deref(&self) -> &Self::Target {
        &self.dataset
    }
}

impl Drop for TempVrt {
    fn drop(&mut self) {
        unsafe {
            let c_string = std::ffi::CString::from_vec_unchecked(self.mem_path.bytes().collect());
            VSIUnlink(c_string.as_ptr());
        }
    }
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

fn index_subdataset<C: TaskContext>(
    conversion: &ConversionMetadata,
    time_coverage: &TimeCoverage,
    resampling_method: Option<ResamplingMethod>,
    task_context: &C,
    conversion_index: usize,
    number_of_conversions: usize,
) -> Result<OverviewGeneration> {
    if conversion.dataset_out_folder.exists() {
        debug!(
            "Skipping conversion: {}",
            conversion.dataset_out_folder.display()
        );
        return Ok(OverviewGeneration::Skipped);
    }

    debug!(
        "Indexing conversion: {}",
        conversion.dataset_out_folder.display()
    );

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

    fs::create_dir_all(conversion.dataset_out_folder.parent().context(
        error::CannotReadInputFileDir {
            path: conversion.dataset_out_folder.clone(),
        },
    )?)
    .boxed_context(error::CannotCreateOverviews)?;

    let cog_driver = gdal::Driver::get("COG").boxed_context(error::CannotCreateOverviews)?;
    let options = CogRasterCreationOptions::new(resampling_method)?;

    debug!("Overview creation GDAL options: {:?}", options.options());

    let number_of_rasters = subdataset.raster_count();
    let number_of_other_dimensions = (number_of_rasters as u32) / conversion.number_of_time_steps;

    let time_steps = time_coverage.time_steps()?;

    for dimension in 1..=number_of_other_dimensions {
        let dimension_folder = conversion.dataset_out_folder.join(dimension.to_string());

        fs::create_dir_all(&dimension_folder).boxed_context(error::CannotCreateOverviews)?;

        emit_subtask_status(
            conversion_index,
            number_of_conversions,
            dimension,
            number_of_other_dimensions,
            task_context,
        );

        for (t, time_step) in time_steps.iter().enumerate() {
            let band = 1 + (dimension - 1) * conversion.number_of_time_steps + (t as u32);

            let vrt = TempVrt::new(&subdataset, band)?;

            let filename = dimension_folder.join(&format!(
                "{}.tiff",
                time_step.as_date_time().map_or_else(
                    || time_step.as_rfc3339(), // TODO: better fallback?
                    |d| d.format(&DateTimeParseFormat::custom(
                        "%Y-%m-%dT%H:%M:%S%.3f".to_string()
                    )),
                )
            ));

            // create COG from VRT
            vrt.create_copy(&cog_driver, &filename, &options.options())
                .boxed_context(error::CannotCreateOverviews)?;

            // create loading info from first time step / band
            if t > 1 {
                continue;
            }

            let first_band_dataset = gdal_open_dataset_ex(
                &filename,
                DatasetOptions {
                    open_flags: GdalOpenFlags::GDAL_OF_READONLY,
                    allowed_drivers: Some(&["COG", "GTiff"]),
                    open_options: None,
                    sibling_files: None,
                },
            )
            .boxed_context(error::CannotOpenNetCdfSubdataset)?;
            let loading_info =
                generate_loading_info(&first_band_dataset, &filename, time_coverage)?;

            write_loading_info(&filename.with_file_name("loading_info.json"), &loading_info)?;
        }
    }

    Ok(OverviewGeneration::Created)
}

fn emit_subtask_status<C: TaskContext>(
    conversion_index: usize,
    number_of_conversions: usize,
    dimension: u32,
    number_of_other_dimensions: u32,
    task_context: &C,
) {
    let min_pct = conversion_index as f64 / number_of_conversions as f64;
    let max_pct = (conversion_index + 1) as f64 / number_of_conversions as f64;
    let dimension_pct = f64::from(dimension - 1) / f64::from(number_of_other_dimensions);
    emit_status(
        task_context,
        min_pct + dimension_pct * (max_pct - min_pct),
        format!("Processing {} of {number_of_conversions} subdatasets; Dimension {dimension} of {number_of_other_dimensions}", conversion_index + 1),
    );
}

fn write_loading_info(file_path: &Path, loading_info: &MetaDataDefinition) -> Result<()> {
    let loading_info_file =
        File::create(file_path).boxed_context(error::CannotWriteMetadataFile)?;
    let mut writer = BufWriter::new(loading_info_file);
    writer
        .write_all(
            serde_json::to_string(&loading_info)
                .boxed_context(error::CannotWriteMetadataFile)?
                .as_bytes(),
        )
        .boxed_context(error::CannotWriteMetadataFile)
}

fn generate_loading_info(
    dataset: &Dataset,
    overview_dataset_path: &Path,
    time_coverage: &TimeCoverage,
) -> Result<MetaDataDefinition> {
    // every raster has a single band
    const BAND: usize = 1;

    let result_descriptor = raster_descriptor_from_dataset(dataset, 1, None)
        .boxed_context(error::CannotGenerateLoadingInfo)?;

    let mut params =
        gdal_parameters_from_dataset(dataset, BAND, overview_dataset_path, Some(BAND), None)
            .boxed_context(error::CannotGenerateLoadingInfo)?;

    Ok(match *time_coverage {
        TimeCoverage::Regular { start, end, step } => {
            let placeholder = "%_START_TIME_%".to_string();
            params.file_path = params
                .file_path
                .with_file_name(&placeholder)
                .with_extension("tiff");

            MetaDataDefinition::GdalMetaDataRegular(GdalMetaDataRegular {
                result_descriptor,
                params,
                time_placeholders: [(
                    placeholder,
                    GdalSourceTimePlaceholder {
                        format: DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%.3f".to_string()),
                        reference: TimeReference::Start,
                    },
                )]
                .into_iter()
                .collect(),
                data_time: TimeInterval::new(start, end)
                    .context(error::InvalidTimeRangeForDataset)?,
                step,
            })
        }
        TimeCoverage::List { ref time_stamps } => {
            let mut params_list = Vec::with_capacity(time_stamps.len());
            for time_instance in time_stamps.iter() {
                let mut params = params.clone();

                params.file_path = params
                    .file_path
                    .with_file_name(time_instance.as_rfc3339())
                    .with_extension("tiff");

                params_list.push(GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::new_instant(*time_instance)
                        .context(error::InvalidTimeCoverageInterval)?,
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        datasets::external::netcdfcf::{
            NetCdfEntity, NetCdfGroup as FullNetCdfGroup, NetCdfOverview,
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
            MetaDataDefinition::GdalMetaDataRegular(GdalMetaDataRegular {
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
                step: TimeStep {
                    granularity: TimeGranularity::Months,
                    step: 1,
                },
                time_placeholders: hashmap! {
                    "%TIME%".to_string() => GdalSourceTimePlaceholder {
                        format: DateTimeParseFormat::custom("%f".to_string()),
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
                dataset_out_folder: dataset_out.clone(),
                number_of_time_steps: 12,
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
            &NopTaskContext,
            0,
            1,
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
            &NopTaskContext,
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
            &NopTaskContext,
        )
        .unwrap();

        assert!(!is_empty(overview_folder.path()));

        remove_overviews(dataset_path, overview_folder.path(), false).unwrap();

        assert!(is_empty(overview_folder.path()));
    }
}
