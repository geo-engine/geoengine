use super::{
    build_netcdf_tree, database::InProgressFlag, error, gdal_netcdf_open, metadata::DataRange,
    NetCdfCf4DProviderError, NetCdfCfProviderDb, NetCdfOverview, TimeCoverage,
};
use crate::{
    datasets::external::netcdfcf::loading::{create_loading_info, ParamModification},
    tasks::{TaskContext, TaskStatusInfo},
    util::{config::get_config_element, path_with_base_path},
};
use gdal::{
    cpl::CslStringList,
    errors::GdalError,
    programs::raster::{
        multi_dim_translate, MultiDimTranslateDestination, MultiDimTranslateOptions,
    },
    raster::{Group, RasterCreationOptions},
    Dataset,
};
use geoengine_datatypes::{
    dataset::DataProviderId, error::BoxedResultExt, primitives::TimeInstance,
    util::gdal::ResamplingMethod,
};
use geoengine_datatypes::{
    primitives::CacheTtlSeconds, spatial_reference::SpatialReference, util::canonicalize_subpath,
};
use geoengine_operators::{
    source::GdalMetaDataList,
    util::gdal::{gdal_parameters_from_dataset, raster_descriptor_from_dataset_and_sref},
};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;

type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

const OVERVIEW_GENERATION_OF_TOTAL_PCT: f64 = 0.9; // just say the last 10% are metadata

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

/// Basic infos for converting a `NetCDF` cube to a COG
#[derive(Debug, Clone)]
struct ConversionMetadata {
    pub dataset_in: String,
    pub dataset_out_base: PathBuf,
    pub array_path: String,
    pub number_of_entities: usize,
    // for database only
    pub file_path: PathBuf,
    pub data_path: Vec<String>,
}

/// Metadata for converting a `NetCDF` cube entity to a COG
#[derive(Debug, Clone)]
struct ConversionMetadataEntity {
    pub base: Arc<ConversionMetadata>,
    pub time_coverage: Arc<TimeCoverage>,
    pub entity: usize,
    pub raster_creation_options: Arc<CogRasterCreationOptionss>,
}

/// Metadata for converting a `NetCDF` cube slice to a COG
#[derive(Debug, Clone)]
struct ConversionMetadataEntityPart {
    pub entity: ConversionMetadataEntity,
    pub time_instance: TimeInstance,
    pub time_idx: usize,
}

impl ConversionMetadataEntityPart {
    fn destination(&self) -> PathBuf {
        self.entity
            .base
            .dataset_out_base
            .join(self.entity.entity.to_string())
            .join(self.time_instance.as_datetime_string_with_millis() + ".tiff")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "status")]
#[allow(clippy::large_enum_variant)]
pub enum OverviewGeneration {
    Created { details: NetCdfOverview },
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
        dataset_path: &Path,
        file_path: &Path,
        out_root_path: &Path,
    ) -> Vec<Arc<ConversionMetadata>> {
        let in_path = file_path.to_string_lossy();
        let mut metadata = Vec::new();

        for (mut data_path, array) in self.flatten() {
            let dataset_out_base = out_root_path.join(data_path.join("/"));

            data_path.push(array.name);
            let array_path = data_path.join("/");

            metadata.push(Arc::new(ConversionMetadata {
                dataset_in: format!("NETCDF:\"{in_path}\""),
                dataset_out_base,
                array_path,
                number_of_entities: array.number_of_entities,
                file_path: dataset_path.to_owned(),
                data_path: {
                    // remove `ebv_cube` suffix
                    data_path.pop();
                    data_path
                },
            }));
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

pub struct OverviewCreationOptions<'a> {
    pub provider_id: DataProviderId,
    pub provider_path: &'a Path,
    pub overview_path: &'a Path,
    pub dataset_path: &'a Path,
    pub resampling_method: Option<ResamplingMethod>,
    /// If true, does not create overviews, only checks if they exist
    pub check_file_only: bool,
}

pub async fn create_overviews<
    C: TaskContext + 'static,
    D: NetCdfCfProviderDb + 'static + std::fmt::Debug,
>(
    task_context: C,
    db: Arc<D>,
    options: OverviewCreationOptions<'_>,
) -> Result<NetCdfOverview> {
    let file_path = canonicalize_subpath(options.provider_path, options.dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;
    let out_folder_path = path_with_base_path(options.overview_path, options.dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;

    fs::create_dir_all(&out_folder_path)
        .await
        .boxed_context(error::InvalidDirectory)?;

    // must have this flag before any write operations
    let in_progress_flag = InProgressFlag::create(
        db.clone(),
        options.provider_id,
        options.dataset_path.to_string_lossy().into(),
    )
    .await?;

    let (time_coverage, conversion_metadata) = create_time_coverage_and_conversion_metadata(
        file_path.clone(),
        out_folder_path.clone(),
        options.dataset_path.to_owned(),
    )
    .await?;

    let number_of_conversions = conversion_metadata.len();
    let mut stats_for_group = HashMap::<String, DataRange>::new();
    let raster_creation_options =
        Arc::new(CogRasterCreationOptionss::new(options.resampling_method)?);
    let mut loading_info_metadatas = Vec::with_capacity(number_of_conversions);

    for (conversion_index, conversion) in conversion_metadata.into_iter().enumerate() {
        debug!(
            "Indexing conversion: {}",
            conversion.dataset_out_base.display()
        );

        let mut subdataset = gdal_netcdf_open(None, Path::new(&conversion.dataset_in))
            .boxed_context(error::CannotOpenNetCdfSubdataset)?;

        debug!(
            "Overview creation GDAL options: {:?}",
            &raster_creation_options
        );

        for entity in 0..conversion.number_of_entities {
            emit_subtask_status(
                conversion_index,
                number_of_conversions,
                entity as u32,
                conversion.number_of_entities as u32,
                &task_context,
            )
            .await;

            let (returned_subdataset, loading_info_metadata) = index_subdataset_entity(
                subdataset,
                ConversionMetadataEntity {
                    base: conversion.clone(),
                    time_coverage: time_coverage.clone(),
                    entity,
                    raster_creation_options: raster_creation_options.clone(),
                },
                &mut stats_for_group,
                options.check_file_only,
            )
            .await?;

            subdataset = returned_subdataset;
            loading_info_metadatas.push(loading_info_metadata);
        }
    }

    emit_status(
        &task_context,
        OVERVIEW_GENERATION_OF_TOTAL_PCT,
        "Collecting metadata".to_string(),
    )
    .await;

    let metadata = build_netcdf_tree(
        options.provider_path,
        options.dataset_path,
        &stats_for_group,
    )?;

    db.store_overview_metadata(options.provider_id, &metadata, loading_info_metadatas)
        .await?;

    in_progress_flag.remove().await?;

    Ok(metadata)
}

async fn create_time_coverage_and_conversion_metadata(
    file_path: PathBuf,
    out_folder_path: PathBuf,
    dataset_path: PathBuf,
) -> Result<(Arc<TimeCoverage>, Vec<Arc<ConversionMetadata>>)> {
    crate::util::spawn_blocking(move || {
        let dataset =
            gdal_netcdf_open(None, &file_path).boxed_context(error::CannotOpenNetCdfDataset)?;

        let root_group = dataset.root_group().context(error::GdalMd)?;
        let group_tree = root_group.group_tree()?;
        let time_coverage = Arc::new(TimeCoverage::from_dimension(&root_group)?);
        let conversion_metadata =
            group_tree.conversion_metadata(&dataset_path, &file_path, &out_folder_path);

        Ok((time_coverage, conversion_metadata))
    })
    .await
    .boxed_context(error::UnexpectedExecution)?
}

async fn emit_status<C: TaskContext>(task_context: &C, pct: f64, status: String) {
    task_context.set_completion(pct, status.boxed()).await;
}

async fn emit_subtask_status<C: TaskContext>(
    conversion_index: usize,
    number_of_conversions: usize,
    entity: u32,
    number_of_other_entities: u32,
    task_context: &C,
) {
    let min_pct = conversion_index as f64 / number_of_conversions as f64;
    let max_pct = (conversion_index + 1) as f64 / number_of_conversions as f64;
    let dimension_pct = f64::from(entity) / f64::from(number_of_other_entities);
    let pct = min_pct + dimension_pct * (max_pct - min_pct);

    emit_status(
        task_context,
        pct * OVERVIEW_GENERATION_OF_TOTAL_PCT,
        format!("Processing {} of {number_of_conversions} subdatasets; Entity {entity} of {number_of_other_entities}", conversion_index + 1),
    ).await;
}

async fn index_subdataset_entity(
    mut subdataset: gdal::Dataset,
    conversion: ConversionMetadataEntity,
    stats_for_group: &mut HashMap<String, DataRange>,
    check_file_only: bool,
) -> Result<(gdal::Dataset, LoadingInfoMetadata)> {
    let entity_directory = conversion
        .base
        .dataset_out_base
        .join(conversion.entity.to_string());
    fs::create_dir_all(entity_directory)
        .await
        .boxed_context(error::CannotCreateOverviews)?;

    let mut first_overview_dataset = None;
    let mut data_range = DataRange::uninitialized();

    for (time_idx, time_instance) in conversion.time_coverage.time_steps().iter().enumerate() {
        let conversion_entity_part = ConversionMetadataEntityPart {
            entity: conversion.clone(),
            time_instance: *time_instance,
            time_idx,
        };
        let mut result = if check_file_only {
            open_subdataset_tiff(&conversion.base.file_path, conversion_entity_part).await?
        } else {
            let (result, returned_subdataset) =
                create_subdataset_tiff(subdataset, conversion_entity_part).await?;

            subdataset = returned_subdataset; // move and return because of not being `Sync`

            result
        };

        let (returned_subdataset, min_max) = subdataset_min_max(result.overview_dataset).await?;
        result.overview_dataset = returned_subdataset; // move and return because of not being `Sync`

        if let Some((min, max)) = min_max {
            data_range.update_min(min);
            data_range.update_max(max);
        }

        if time_idx == 0 {
            first_overview_dataset = Some((
                result.overview_dataset,
                result.overview_destination,
                result.sref,
            ));
        }
    }

    let Some((overview_dataset, overview_destination, sref)) = first_overview_dataset else {
        return Err(NetCdfCf4DProviderError::NoOverviewsGeneratedForSource {
            path: conversion
                .base
                .dataset_out_base
                .to_string_lossy()
                .to_string(),
        });
    };

    let loading_info = {
        let time_coverage = conversion.time_coverage.clone();

        crate::util::spawn_blocking(move || {
            generate_loading_info(
                &overview_dataset,
                &overview_destination,
                &time_coverage,
                sref,
            )
        })
        .await
        .boxed_context(error::UnexpectedExecution)??
    };

    let loading_info_metadata = LoadingInfoMetadata {
        group_path: conversion.base.data_path.clone(),
        entity_id: conversion.entity,
        meta_data: loading_info,
    };

    stats_for_group
        .entry(conversion.base.data_path.join("/"))
        .or_insert(data_range)
        .update(data_range);

    Ok((subdataset, loading_info_metadata))
}

pub struct LoadingInfoMetadata {
    pub group_path: Vec<String>,
    pub entity_id: usize,
    pub meta_data: GdalMetaDataList,
}

struct CreateSubdatasetTiffResult {
    overview_dataset: Dataset,
    overview_destination: PathBuf,
    sref: SpatialReference,
}

async fn open_subdataset_tiff(
    dataset: &Path,
    conversion_entity_part: ConversionMetadataEntityPart,
) -> Result<CreateSubdatasetTiffResult> {
    let overview_destination = conversion_entity_part.destination();

    let (dataset_result, overview_destination) = crate::util::spawn_blocking(move || {
        (Dataset::open(&overview_destination), overview_destination)
    })
    .await
    .boxed_context(error::UnexpectedExecution)?;

    if let Ok(dataset) = dataset_result {
        Ok(CreateSubdatasetTiffResult {
            sref: subdataset_sref(&dataset, &conversion_entity_part)?,
            overview_dataset: dataset,
            overview_destination,
        })
    } else {
        Err(NetCdfCf4DProviderError::OverviewMissingForRefresh {
            dataset: dataset.to_owned(),
            missing: overview_destination,
        })
    }
}

async fn subdataset_min_max(dataset: Dataset) -> Result<(Dataset, Option<(f64, f64)>)> {
    let (min_max, dataset) = crate::util::spawn_blocking(move || {
        let Ok(band) = dataset.rasterband(1) else {
            return (None, dataset);
        };
        let Ok(stats) = band.compute_raster_min_max(false) else {
            return (None, dataset);
        };
        (Some((stats.min, stats.max)), dataset)
    })
    .await
    .boxed_context(error::UnexpectedExecution)?;

    Ok((dataset, min_max))
}

async fn create_subdataset_tiff(
    subdataset: Dataset,
    conversion: ConversionMetadataEntityPart,
) -> Result<(CreateSubdatasetTiffResult, Dataset)> {
    crate::util::spawn_blocking(move || {
        #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
        _create_subdataset_tiff(&subdataset, &conversion).map(|result| (result, subdataset))
    })
    .await
    .boxed_context(error::UnexpectedExecution)?
}

fn _create_subdataset_tiff(
    subdataset: &Dataset,
    conversion: &ConversionMetadataEntityPart,
) -> Result<CreateSubdatasetTiffResult> {
    let destination = conversion.destination();
    let name = format!("/{}", conversion.entity.base.array_path);
    let view = format!(
        "[{entity},{time_idx},:,:]",
        entity = conversion.entity.entity,
        time_idx = conversion.time_idx
    );
    let mut options = vec![
        "-array".to_string(),
        format!("name={name},view={view}"),
        "-of".to_string(),
        "COG".to_string(),
    ];

    let input_sref = subdataset_sref(subdataset, conversion)?;

    for raster_creation_option in &conversion.entity.raster_creation_options.options()? {
        options.push("-co".to_string());
        options.push(raster_creation_option.to_string());
    }
    let overview_dataset = multi_dim_translate(
        &[subdataset],
        MultiDimTranslateDestination::path(&destination).context(error::GdalMd)?,
        Some(MultiDimTranslateOptions::new(options).context(error::GdalMd)?),
    )
    .context(error::GdalMd)?;

    Ok(CreateSubdatasetTiffResult {
        overview_dataset,
        overview_destination: destination,
        sref: input_sref,
    })
}

fn subdataset_sref(
    subdataset: &Dataset,
    conversion: &ConversionMetadataEntityPart,
) -> Result<SpatialReference> {
    // try to get the spatial reference directly from the dataset
    if let Ok(Ok(spatial_ref)) = subdataset
        .spatial_ref()
        .map(TryInto::<SpatialReference>::try_into)
    {
        return Ok(spatial_ref);
    }

    // open the concrete dataset to get the spatial reference
    let temp_ds = geoengine_operators::util::gdal::gdal_open_dataset(Path::new(&format!(
        "{}:{}",
        conversion.entity.base.dataset_in, conversion.entity.base.array_path
    )))
    .boxed_context(error::CannotOpenNetCdfSubdataset)?;

    temp_ds
        .spatial_ref()
        .context(error::MissingCrs)?
        .try_into()
        .context(error::CannotConvertSRefFromGdal)
}

#[derive(Debug, Clone)]
struct CogRasterCreationOptionss {
    compression_format: String,
    compression_level: String,
    num_threads: String,
    resampling_method: String,
}

impl CogRasterCreationOptionss {
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

impl CogRasterCreationOptionss {
    fn options(&self) -> Result<RasterCreationOptions> {
        const COG_BLOCK_SIZE: &str = "512";

        fn inner(this: &CogRasterCreationOptionss) -> Result<RasterCreationOptions, GdalError> {
            let mut options = RasterCreationOptions::new();
            options.add_name_value("COMPRESS", &this.compression_format)?;
            options.add_name_value("TILED", "YES")?;
            options.add_name_value("LEVEL", &this.compression_level)?;
            options.add_name_value("NUM_THREADS", &this.num_threads)?;
            options.add_name_value("BLOCKSIZE", COG_BLOCK_SIZE)?;
            options.add_name_value("BIGTIFF", "IF_SAFER")?;
            options.add_name_value("RESAMPLING", &this.resampling_method)?; // TODO: test if this suffices

            Ok(options)
        }

        inner(self).context(error::OpeningDatasetForWriting)
    }
}

fn generate_loading_info(
    dataset: &Dataset,
    overview_dataset_path: &Path,
    time_coverage: &TimeCoverage,
    sref: SpatialReference,
) -> Result<GdalMetaDataList> {
    const TIFF_BAND_INDEX: usize = 1;

    let result_descriptor = raster_descriptor_from_dataset_and_sref(dataset, 1, sref)
        .boxed_context(error::CannotGenerateLoadingInfo)?;

    let params = gdal_parameters_from_dataset(
        dataset,
        TIFF_BAND_INDEX,
        overview_dataset_path,
        Some(TIFF_BAND_INDEX),
        None,
    )
    .boxed_context(error::CannotGenerateLoadingInfo)?;

    // we change the cache ttl when returning the overview metadata in the provider
    let cache_ttl = CacheTtlSeconds::default();

    Ok(create_loading_info(
        result_descriptor,
        &params,
        time_coverage
            .time_steps()
            .iter()
            .map(|time_instance| ParamModification::File {
                file_path: params.file_path.clone(),
                time_instance: *time_instance,
            }),
        cache_ttl,
    ))
}

pub async fn remove_overviews<D: NetCdfCfProviderDb + 'static + std::fmt::Debug>(
    provider_id: DataProviderId,
    dataset_path: &Path,
    overview_path: &Path,
    db: Arc<D>,
    force: bool,
) -> Result<()> {
    let out_folder_path = path_with_base_path(overview_path, dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;

    let in_progress_flag = InProgressFlag::create(
        db.clone(),
        provider_id,
        dataset_path.to_string_lossy().into(),
    )
    .await;

    if !force && in_progress_flag.is_err() {
        return Err(NetCdfCf4DProviderError::CannotRemoveOverviewsWhileCreationIsInProgress);
    }

    // entries from other tables will be deleted by the foreign key constraint `ON DELETE CASCADE`
    db.remove_overviews(provider_id, &dataset_path.to_string_lossy())
        .await?;

    if tokio::fs::try_exists(&out_folder_path)
        .await
        .unwrap_or(false)
    {
        tokio::fs::remove_dir_all(&out_folder_path)
            .await
            .boxed_context(error::CannotRemoveOverviews)?;
    }

    if let Ok(in_progress_flag) = in_progress_flag {
        in_progress_flag.remove().await?;
    } else {
        // we will remove the flag in force-mode
        db.unlock_overview(provider_id, &dataset_path.to_string_lossy())
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasets::external::netcdfcf::database::NetCdfCfProviderDb;
    use crate::datasets::external::netcdfcf::NETCDF_CF_PROVIDER_ID;
    use crate::pro::contexts::{PostgresContext, PostgresDb, PostgresSessionContext};
    use crate::{contexts::SessionContext, ge_context, tasks::util::NopTaskContext};
    use gdal::{DatasetOptions, GdalOpenFlags};
    use geoengine_datatypes::{
        primitives::{DateTime, SpatialResolution, TimeInterval},
        raster::RasterDataType,
        spatial_reference::SpatialReference,
        test_data,
        util::gdal::hide_gdal_errors,
    };
    use geoengine_operators::{
        engine::{RasterBandDescriptors, RasterResultDescriptor},
        source::{
            FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
            GdalLoadingInfoTemporalSlice, GdalMetaDataList,
        },
        util::gdal::gdal_open_dataset_ex,
    };
    use tokio_postgres::NoTls;

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
                time_stamps: vec![
                    DateTime::new_utc(2020, 1, 1, 0, 0, 0).into(),
                    DateTime::new_utc(2020, 2, 1, 0, 0, 0).into(),
                ],
            },
            dataset.spatial_ref().unwrap().try_into().unwrap(),
        )
        .unwrap();

        assert_eq!(
            loading_info,
            GdalMetaDataList {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: Some(SpatialResolution::new_unchecked(1.0, 1.0)),
                    bands: RasterBandDescriptors::new_single_band(),
                },
                params: vec![
                    GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new(
                            DateTime::new_utc(2020, 1, 1, 0, 0, 0),
                            DateTime::new_utc(2020, 1, 1, 0, 0, 0)
                        )
                        .unwrap(),
                        params: Some(GdalDatasetParameters {
                            file_path: Path::new("foo/2020-01-01T00:00:00.000Z.tiff").into(),
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
                            retry: None,
                        }),
                        cache_ttl: CacheTtlSeconds::default(),
                    },
                    GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new(
                            DateTime::new_utc(2020, 2, 1, 0, 0, 0),
                            DateTime::new_utc(2020, 2, 1, 0, 0, 0)
                        )
                        .unwrap(),
                        params: Some(GdalDatasetParameters {
                            file_path: Path::new("foo/2020-02-01T00:00:00.000Z.tiff").into(),
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
                            retry: None,
                        }),
                        cache_ttl: CacheTtlSeconds::default(),
                    },
                ],
            }
        );
    }

    #[ge_context::test]
    async fn test_create_overviews(
        _app_ctx: PostgresContext<NoTls>,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let db = Arc::new(ctx.db());

        create_overviews(
            NopTaskContext,
            db,
            OverviewCreationOptions {
                provider_id: NETCDF_CF_PROVIDER_ID,
                provider_path: test_data!("netcdf4d"),
                overview_path: overview_folder.path(),
                dataset_path: Path::new("dataset_m.nc"),
                resampling_method: None,
                check_file_only: false,
            },
        )
        .await
        .unwrap();

        let dataset_folder = overview_folder.path().join("dataset_m.nc");

        assert!(dataset_folder.is_dir());

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
            }
        }
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn test_create_overviews_irregular(
        _app_ctx: PostgresContext<NoTls>,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let db: Arc<PostgresDb<NoTls>> = Arc::new(ctx.db());

        create_overviews(
            NopTaskContext,
            db.clone(),
            OverviewCreationOptions {
                provider_id: NETCDF_CF_PROVIDER_ID,
                provider_path: test_data!("netcdf4d"),
                overview_path: overview_folder.path(),
                dataset_path: Path::new("dataset_irr_ts.nc"),
                resampling_method: None,
                check_file_only: false,
            },
        )
        .await
        .unwrap();

        let dataset_folder = overview_folder.path().join("dataset_irr_ts.nc");

        assert!(dataset_folder.is_dir());

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
            }
        }

        let sample_loading_info: GdalMetaDataList = db
            .loading_info(
                NETCDF_CF_PROVIDER_ID,
                "dataset_irr_ts.nc",
                &["metric_2".to_string()],
                0,
            )
            .await
            .unwrap()
            .unwrap();
        pretty_assertions::assert_eq!(
            sample_loading_info,
            GdalMetaDataList {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I16,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: Some(SpatialResolution::new_unchecked(1.0, 1.0)),
                    bands: RasterBandDescriptors::new_single_band(),
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
                            retry: None,
                        }),
                        cache_ttl: CacheTtlSeconds::default(),
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
                            retry: None,
                        }),
                        cache_ttl: CacheTtlSeconds::default(),
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
                            retry: None,
                        }),
                        cache_ttl: CacheTtlSeconds::default(),
                    }
                ],
            }
        );
    }

    #[ge_context::test]
    async fn test_remove_overviews(
        _app_ctx: PostgresContext<NoTls>,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        fn is_empty(directory: &Path) -> bool {
            directory.read_dir().unwrap().next().is_none()
        }

        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let dataset_path = Path::new("dataset_m.nc");

        let db = Arc::new(ctx.db());

        assert!(!db
            .overviews_exist(
                NETCDF_CF_PROVIDER_ID,
                dataset_path.to_string_lossy().as_ref()
            )
            .await
            .unwrap());

        create_overviews(
            NopTaskContext,
            db.clone(),
            OverviewCreationOptions {
                provider_id: NETCDF_CF_PROVIDER_ID,
                provider_path: test_data!("netcdf4d"),
                overview_path: overview_folder.path(),
                dataset_path,
                resampling_method: None,
                check_file_only: false,
            },
        )
        .await
        .unwrap();

        assert!(!is_empty(overview_folder.path()));

        remove_overviews(
            NETCDF_CF_PROVIDER_ID,
            dataset_path,
            overview_folder.path(),
            db.clone(),
            false,
        )
        .await
        .unwrap();

        assert!(is_empty(overview_folder.path()));

        assert!(!db
            .overviews_exist(
                NETCDF_CF_PROVIDER_ID,
                dataset_path.to_string_lossy().as_ref()
            )
            .await
            .unwrap());
    }
}
