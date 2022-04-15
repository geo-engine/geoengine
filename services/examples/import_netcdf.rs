use std::{
    collections::HashSet,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use gdal::{raster::RasterCreationOption, DatasetOptions, GdalOpenFlags};
use geoengine_operators::util::gdal::gdal_open_dataset_ex;
use geoengine_services::{
    datasets::external::netcdfcf::{
        gdalmd::MdGroup, NetCdfCf4DProviderError, NetCdfCfDataProvider,
    },
    test_data,
    util::{config::get_config_element, tests::initialize_debugging_in_test},
};
use log::debug;
use snafu::{OptionExt, ResultExt};
use std::fs;

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
    fn group_tree(&self) -> Result<NetCdfGroup, NetCdfCf4DProviderError>;
}

impl NetCdfVisitor for MdGroup<'_> {
    fn group_tree(&self) -> Result<NetCdfGroup, NetCdfCf4DProviderError> {
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

fn main() {
    initialize_debugging_in_test();

    let file_path = test_data!("netcdf4d/dataset_sm.nc").to_path_buf();
    let file_name = file_path.file_name().unwrap().to_string_lossy();
    let out_folder_path = test_data!("netcdf4d/out").join(format!("{file_name}_index"));

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

    eprintln!("{:#?}", group_tree);

    eprintln!("{:#?}", group_tree.flatten());

    eprintln!(
        "{:#?}",
        group_tree.conversion_metadata(&file_path, &out_folder_path)
    );

    store_metadata(&file_path, &out_folder_path).unwrap();

    for conversion in group_tree.conversion_metadata(&file_path, &out_folder_path) {
        index_subdataset(conversion).unwrap();
    }
}

// TODO: change `Whatever` to custom error type
fn store_metadata(file_path: &Path, out_folder_path: &Path) -> Result<(), snafu::Whatever> {
    let out_file_path = out_folder_path.join("metadata.json");

    if out_file_path.exists() {
        debug!("Skipping metadata generation: {}", file_path.display());
        return Ok(());
    }

    debug!("Creating metadata: {}", file_path.display());

    let metadata = NetCdfCfDataProvider::build_netcdf_tree(
        file_path
            .parent()
            .whatever_context("Input dataset has no containing directory")?,
        file_path
            .file_name()
            .map(Path::new)
            .whatever_context("Input dataset has no filename")?,
    )
    .whatever_context("Could not generate metadata for dataset")?;

    fs::create_dir_all(out_folder_path)
        .whatever_context("Cannot create directory path to output file")?;

    let file = File::create(out_file_path).whatever_context("Cannot create output file")?;

    let mut writer = BufWriter::new(file);

    writer
        .write_all(
            serde_json::to_string(&metadata)
                .whatever_context("Cannot serialize metadata")?
                .as_bytes(),
        )
        .whatever_context("Cannot write metadata to output file")?;

    Ok(())
}

// TODO: change `Whatever` to custom error type
fn index_subdataset(conversion: ConversionMetadata) -> Result<(), snafu::Whatever> {
    if conversion.dataset_out.exists() {
        debug!("Skipping conversion: {}", conversion.dataset_out.display());
        return Ok(());
    } else {
        debug!("Indexing conversion: {}", conversion.dataset_out.display());
    }
    let subdataset = gdal_open_dataset_ex(
        Path::new(&conversion.dataset_in),
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY,
            allowed_drivers: Some(&["netCDF"]),
            open_options: None,
            sibling_files: None,
        },
    )
    .whatever_context("Cannot open NetCDF subdataset")?;

    fs::create_dir_all(
        conversion
            .dataset_out
            .parent()
            .whatever_context("Output dataset has no containing directory")?,
    )
    .whatever_context("Cannot create directory path to output file")?;

    const COG_BLOCK_SIZE: &str = "512";
    const COMPRESSION_FORMAT: &str = "LZW";
    const COMPRESSION_LEVEL: &str = "9";

    let gdal_num_threads = get_config_element::<geoengine_services::util::config::Gdal>()
        .whatever_context("Cannot read Geo Engine's GDAL config")?
        .compression_num_threads;
    let num_threads = gdal_num_threads.to_string();

    let cog_driver = gdal::Driver::get("COG").whatever_context("There is no COG driver")?;
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
        .whatever_context("cannot convert subdataset")?;

    Ok(())
}
