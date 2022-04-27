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

    // TODO: don't unwrap

    let dataset = gdal_open_dataset_ex(
        &file_path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
            allowed_drivers: Some(&["netCDF"]),
            open_options: None,
            sibling_files: None,
        },
    )
    .unwrap();

    let root_group = MdGroup::from_dataset(&dataset).unwrap();

    let group_tree = root_group.group_tree().unwrap();

    store_metadata(provider_path, dataset_path, &out_folder_path).unwrap();

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
            .context(error::CannotReadInputFileDir)?,
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
    // TODO: store `loading_info` as json file

    Ok(())
}

fn generate_loading_info(
    dataset: &Dataset,
    overview_dataset_path: &Path,
    time_coverage: &TimeCoverage,
) -> Result<GdalMetadataNetCdfCf> {
    // TODO: get measurement from NetCdf
    let result_descriptor = raster_descriptor_from_dataset(dataset, 1, None)
        .boxed_context(error::CannotGenerateLoadingInfo)?;

    let params = gdal_parameters_from_dataset(
        dataset,
        0,
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
