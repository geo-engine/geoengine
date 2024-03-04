use super::{error, gdal_netcdf_open, metadata::DataRange, NetCdfCf4DProviderError, TimeCoverage};
use crate::{
    datasets::external::netcdfcf::{
        loading::{create_loading_info, ParamModification},
        NetCdfCfDataProvider,
    },
    tasks::{TaskContext, TaskStatusInfo},
    util::{config::get_config_element, path_with_base_path},
};
use gdal::{
    cpl::CslStringList,
    programs::raster::{
        multi_dim_translate, MultiDimTranslateDestination, MultiDimTranslateOptions,
    },
    raster::{Group, RasterBand, RasterCreationOption},
    Dataset,
};
use gdal_sys::GDALGetRasterStatistics;
use geoengine_datatypes::{
    error::BoxedResultExt, primitives::TimeInstance, util::gdal::ResamplingMethod,
};
use geoengine_datatypes::{
    primitives::CacheTtlSeconds, spatial_reference::SpatialReference, util::canonicalize_subpath,
};
use geoengine_operators::{
    source::GdalMetaDataList,
    util::gdal::{
        gdal_parameters_from_dataset, raster_descriptor_from_dataset,
        raster_descriptor_from_dataset_and_sref,
    },
};
use log::debug;
use snafu::ResultExt;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use tokio::fs;
use tokio_postgres::Transaction;

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
        dataset_path: &Path,
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
                file_path: dataset_path.to_owned(),
                data_path: {
                    // remove `ebv_cube` suffix
                    data_path.pop();
                    data_path
                },
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

pub async fn create_overviews<C: TaskContext + 'static>(
    provider_path: &Path,
    dataset_path: &Path,
    overview_path: &Path,
    resampling_method: Option<ResamplingMethod>,
    task_context: C,
    db_transaction: &Transaction<'_>,
) -> Result<OverviewGeneration> {
    if overview_exists(dataset_path, db_transaction).await? {
        debug!("Skipping conversion: {}", dataset_path.display());
        return Ok(OverviewGeneration::Skipped);
    }

    let file_path = canonicalize_subpath(provider_path, dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;
    let out_folder_path = path_with_base_path(overview_path, dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;

    if !exists(&out_folder_path).await? {
        fs::create_dir_all(&out_folder_path)
            .await
            .boxed_context(error::InvalidDirectory)?;
    }

    // must have this flag before any write operations
    let in_progress_flag = InProgressFlag::create(&out_folder_path).await?;

    let (time_coverage, conversion_metadata) = {
        let dataset_path = dataset_path.to_owned();

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
        .boxed_context(error::UnexpectedExecution)??
    };

    let number_of_conversions = conversion_metadata.len();
    let mut stats_for_group = HashMap::<String, DataRange>::new();

    for (i, conversion) in conversion_metadata.into_iter().enumerate() {
        match index_subdataset(
            Arc::new(conversion),
            time_coverage.clone(),
            resampling_method,
            &task_context,
            &mut stats_for_group,
            (i, number_of_conversions),
            db_transaction,
        )
        .await
        {
            Ok(OverviewGeneration::Created) => (),
            Ok(OverviewGeneration::Skipped) => {
                return Ok(OverviewGeneration::Skipped);
            }
            Err(e) => return Err(e),
        }
    }

    emit_status(
        &task_context,
        OVERVIEW_GENERATION_OF_TOTAL_PCT,
        "Collecting metadata".to_string(),
    )
    .await;

    match store_db_metadata(
        provider_path,
        dataset_path,
        &stats_for_group,
        db_transaction,
    )
    .await
    {
        Ok(OverviewGeneration::Created) => (),
        Ok(OverviewGeneration::Skipped) => return Ok(OverviewGeneration::Skipped),
        Err(e) => return Err(e),
    };

    in_progress_flag.remove().await?;

    Ok(OverviewGeneration::Created)
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

/// A flag that indicates on-going process of an overview folder.
///
/// Cleans up the folder if dropped.
pub struct InProgressFlag {
    path: PathBuf,
}

impl InProgressFlag {
    const IN_PROGRESS_FLAG_NAME: &'static str = ".in_progress";

    async fn create(folder: &Path) -> Result<Self> {
        let folder = folder.to_owned();
        crate::util::spawn_blocking(move || {
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
        })
        .await
        .boxed_context(error::UnexpectedExecution)?
    }

    async fn remove(self) -> Result<()> {
        fs::remove_file(self.path.as_path())
            .await
            .boxed_context(error::CannotRemoveInProgressFlag)?;
        Ok(())
    }

    pub async fn is_in_progress(folder: &Path) -> Result<bool> {
        let folder = folder.to_owned();
        crate::util::spawn_blocking(move || {
            if !folder.is_dir() {
                return false;
            }

            let path = folder.join(Self::IN_PROGRESS_FLAG_NAME);

            path.exists()
        })
        .await
        .boxed_context(error::UnexpectedExecution)
    }
}

impl Drop for InProgressFlag {
    fn drop(&mut self) {
        if !self.path.exists() {
            return;
        }

        if let Err(e) =
            std::fs::remove_file(&self.path).boxed_context(error::CannotRemoveInProgressFlag)
        {
            log::error!("Cannot remove in progress flag: {}", e);
        }
    }
}

async fn store_db_metadata(
    provider_path: &Path,
    dataset_path: &Path,
    stats_for_group: &HashMap<String, DataRange>,
    db_transaction: &Transaction<'_>,
) -> Result<OverviewGeneration> {
    let metadata =
        NetCdfCfDataProvider::build_netcdf_tree(provider_path, dataset_path, stats_for_group)?;

    // TODO: should we have an error here instead?
    db_transaction
        .execute(
            "DELETE FROM overviews WHERE file_name = $1",
            &[&metadata.file_name],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    db_transaction
        .execute(
            "INSERT INTO overviews (
                            file_name,
                            title,
                            summary,
                            spatial_reference,
                            colorizer,
                            creator_name,
                            creator_email,
                            creator_institution
                        ) VALUES (
                            $1,
                            $2,
                            $3,
                            $4,
                            $5,
                            $6,
                            $7,
                            $8
                        );",
            &[
                &metadata.file_name,
                &metadata.title,
                &metadata.summary,
                &metadata.spatial_reference,
                &metadata.colorizer,
                &metadata.creator_name,
                &metadata.creator_email,
                &metadata.creator_institution,
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    store_db_metadata_groups(&metadata.groups, &metadata.file_name, db_transaction).await?;

    let entity_statement = db_transaction
        .prepare_typed(
            "INSERT INTO entities (
            file_name,
            id,
            name
        ) VALUES (
            $1,
            $2,
            $3
        );",
            &[
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::INT8,
                tokio_postgres::types::Type::TEXT,
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    for entity in &metadata.entities {
        db_transaction
            .execute(
                &entity_statement,
                &[&metadata.file_name, &(entity.id as i64), &entity.name],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;
    }

    let timestamp_statement = db_transaction
        .prepare_typed(
            r#"INSERT INTO timestamps (
            file_name,
            "time"
        ) VALUES (
            $1,
            $2
        );"#,
            &[
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::INT8,
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    for time_step in metadata.time_coverage.time_steps() {
        db_transaction
            .execute(&timestamp_statement, &[&metadata.file_name, &time_step])
            .await
            .boxed_context(error::UnexpectedExecution)?;
    }

    Ok(OverviewGeneration::Created)
}

async fn store_db_metadata_groups(
    groups: &[crate::datasets::external::netcdfcf::NetCdfGroup],
    file_name: &str,
    db_transaction: &Transaction<'_>,
) -> Result<()> {
    let group_statement = db_transaction
        .prepare_typed(
            "INSERT INTO groups (
        file_name,
        name,
        title,
        description,
        data_range,
        unit,
        data_type
    ) VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7
    );",
            &[
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT_ARRAY,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::FLOAT8_ARRAY,
                tokio_postgres::types::Type::TEXT,
                // we omit `data_type`
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    let mut group_stack = groups
        .iter()
        .map(|group| (vec![group.name.as_str()], group))
        .collect::<Vec<_>>();

    while let Some((group_path, group)) = group_stack.pop() {
        for subgroup in &group.groups {
            let mut subgroup_path = group_path.clone();
            subgroup_path.push(subgroup.name.as_str());
            group_stack.push((subgroup_path, subgroup));
        }

        db_transaction
            .execute(
                &group_statement,
                &[
                    &file_name,
                    &group_path,
                    &group.title,
                    &group.description,
                    &group.data_range,
                    &group.unit,
                    &group.data_type,
                ],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;
    }

    Ok(())
}

async fn overview_exists(dataset_path: &Path, db_transaction: &Transaction<'_>) -> Result<bool> {
    db_transaction
        .query_opt(
            "SELECT 1 FROM overviews WHERE file_name = $1",
            &[&dataset_path.to_string_lossy()],
        )
        .await
        .boxed_context(error::UnexpectedExecution)
        .map(|row| row.is_some())
}

async fn index_subdataset<C: TaskContext>(
    conversion: Arc<ConversionMetadata>,
    time_coverage: Arc<TimeCoverage>,
    resampling_method: Option<ResamplingMethod>,
    task_context: &C,
    stats_for_group: &mut HashMap<String, DataRange>,
    (conversion_index, number_of_conversions): (usize, usize),
    db_transaction: &Transaction<'_>,
) -> Result<OverviewGeneration> {
    if overview_exists(&conversion.file_path, db_transaction).await? {
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

    let mut subdataset = gdal_netcdf_open(None, Path::new(&conversion.dataset_in))
        .boxed_context(error::CannotOpenNetCdfSubdataset)?;

    let raster_creation_options = Arc::new(CogRasterCreationOptions::new(resampling_method)?);

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
            task_context,
        )
        .await;

        let loading_info_stmt = db_transaction
            .prepare_typed(
                r#"
            INSERT INTO loading_infos (
                file_name,
                group_names,
                entity_id,
                meta_data
            ) VALUES (
                $1,
                $2 :: TEXT[],
                $3,
                $4 :: "GdalMetaDataList"
            );"#,
                &[
                    tokio_postgres::types::Type::TEXT,
                    tokio_postgres::types::Type::TEXT_ARRAY,
                    tokio_postgres::types::Type::INT8,
                    // we omit `GdalMetaDataList`
                ],
            )
            .await
            .boxed_context(error::CannotStoreLoadingInfo)?;

        subdataset = index_subdataset_entity(
            conversion.clone(),
            time_coverage.clone(),
            stats_for_group,
            (db_transaction, &loading_info_stmt),
            entity,
            raster_creation_options.clone(),
            subdataset,
        )
        .await?;
    }

    Ok(OverviewGeneration::Created)
}

async fn index_subdataset_entity(
    conversion: Arc<ConversionMetadata>,
    time_coverage: Arc<TimeCoverage>,
    stats_for_group: &mut HashMap<String, DataRange>,
    (db_transaction, loading_info_stmt): (&Transaction<'_>, &tokio_postgres::Statement),
    entity: usize,
    raster_creation_options: Arc<CogRasterCreationOptions>,
    mut subdataset: gdal::Dataset,
) -> Result<gdal::Dataset> {
    let entity_directory = conversion.dataset_out_base.join(entity.to_string());
    fs::create_dir_all(entity_directory)
        .await
        .boxed_context(error::CannotCreateOverviews)?;

    let (first_overview_dataset, subdataset_sref_string, data_range_candidate) = {
        let raster_creation_options = raster_creation_options.clone();
        let conversion = conversion.clone();
        let time_coverage = time_coverage.clone();

        let (
            returned_subdataset,
            first_overview_dataset,
            subdataset_sref_string,
            data_range_candidate,
        ) = crate::util::spawn_blocking(move || {
            let mut data_range = DataRange::uninitialized();
            let mut first_overview_dataset = None;

            let mut subdataset_sref_string = None;

            for (time_idx, time_step) in time_coverage.time_steps().iter().enumerate() {
                let CreateSubdatasetTiffResult {
                    overview_dataset,
                    overview_destination,
                    min_max,
                    sref_string,
                } = create_subdataset_tiff(
                    *time_step,
                    &conversion,
                    entity,
                    &raster_creation_options.options(),
                    &subdataset,
                    time_idx,
                )?;

                if let Some((min, max)) = min_max {
                    data_range.update_min(min);
                    data_range.update_max(max);
                }
                if time_idx == 0 {
                    first_overview_dataset = Some((overview_dataset, overview_destination));
                }

                if let Some(sref) = sref_string {
                    subdataset_sref_string = Some(sref);
                }
            }

            Ok((
                subdataset,
                first_overview_dataset,
                subdataset_sref_string,
                data_range,
            ))
        })
        .await
        .boxed_context(error::UnexpectedExecution)??;

        subdataset = returned_subdataset;

        (
            first_overview_dataset,
            subdataset_sref_string,
            data_range_candidate,
        )
    };

    let Some((overview_dataset, overview_destination)) = first_overview_dataset else {
        return Err(NetCdfCf4DProviderError::NoOverviewsGeneratedForSource {
            path: conversion.dataset_out_base.to_string_lossy().to_string(),
        });
    };

    let loading_info = {
        let time_coverage = time_coverage.clone();

        crate::util::spawn_blocking(move || {
            generate_loading_info(
                &overview_dataset,
                &overview_destination,
                &time_coverage,
                subdataset_sref_string.clone(),
            )
        })
        .await
        .boxed_context(error::UnexpectedExecution)??
    };

    db_transaction
        .execute(
            loading_info_stmt,
            &[
                &conversion.file_path.to_string_lossy(),
                &conversion.data_path,
                &(entity as i64),
                &loading_info,
            ],
        )
        .await
        .boxed_context(error::CannotStoreLoadingInfo)?;

    // remove array from path and insert to `stats_for_group`
    if let Some((array_path_stripped, _)) = conversion.array_path.rsplit_once('/') {
        stats_for_group
            .entry(array_path_stripped.to_string())
            .or_insert(data_range_candidate)
            .update(data_range_candidate);
    }

    Ok(subdataset)
}

struct CreateSubdatasetTiffResult {
    overview_dataset: Dataset,
    overview_destination: PathBuf,
    min_max: Option<(f64, f64)>,
    sref_string: Option<String>,
}

fn create_subdataset_tiff(
    time_step: TimeInstance,
    conversion: &ConversionMetadata,
    entity: usize,
    raster_creation_options: &Vec<RasterCreationOption>,
    subdataset: &Dataset,
    time_idx: usize,
) -> Result<CreateSubdatasetTiffResult> {
    let time_str = time_step.as_datetime_string_with_millis();
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

    let input_sref_string = {
        // open the concrete dataset to get the spatial reference. This does not work on the `subdataset`.
        let temp_ds = geoengine_operators::util::gdal::gdal_open_dataset(Path::new(&format!(
            "{}:{}",
            conversion.dataset_in, conversion.array_path
        )))
        .boxed_context(error::CannotOpenNetCdfSubdataset)?;

        temp_ds
            .spatial_ref()
            .context(error::MissingCrs)?
            .authority()
            .ok()
    };

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
        sref_string: input_sref_string,
    })
}

#[derive(Debug, Clone)]
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
    sref_string: Option<String>,
) -> Result<GdalMetaDataList> {
    const TIFF_BAND_INDEX: usize = 1;

    let result_descriptor = if let Some(sref) = sref_string {
        let spatial_ref =
            SpatialReference::from_str(&sref).boxed_context(error::CannotGenerateLoadingInfo)?;

        raster_descriptor_from_dataset_and_sref(dataset, 1, spatial_ref)
            .boxed_context(error::CannotGenerateLoadingInfo)?
    } else {
        raster_descriptor_from_dataset(dataset, 1)
            .boxed_context(error::CannotGenerateLoadingInfo)?
    };

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

pub async fn remove_overviews(
    dataset_path: &Path,
    overview_path: &Path,
    db_transaction: &Transaction<'_>,
    force: bool,
) -> Result<()> {
    let out_folder_path = path_with_base_path(overview_path, dataset_path)
        .boxed_context(error::DatasetIsNotInProviderPath)?;

    if !exists(&out_folder_path).await? {
        return Ok(());
    }

    if !force && InProgressFlag::is_in_progress(&out_folder_path).await? {
        return Err(NetCdfCf4DProviderError::CannotRemoveOverviewsWhileCreationIsInProgress);
    }

    // entries from other tables will be deleted by the foreign key constraint `ON DELETE CASCADE`
    db_transaction
        .execute(
            "DELETE FROM overviews WHERE file_name = $1",
            &[&dataset_path.to_string_lossy()],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    tokio::fs::remove_dir_all(&out_folder_path)
        .await
        .boxed_context(error::CannotRemoveOverviews)?;

    Ok(())
}

pub async fn meta_data_from_overviews(
    db_transaction: &Transaction<'_>,
    dataset_path: &str,
    groups: Vec<String>,
    entity: usize,
) -> Result<Option<GdalMetaDataList>> {
    let row = db_transaction
        .query_opt(
            r#"
            SELECT meta_data :: "GdalMetaDataList"
            FROM loading_infos
            WHERE
                file_name = $1 AND
                group_names = $2 :: TEXT[] AND
                entity_id = $3
            "#,
            &[&dataset_path, &groups, &(entity as i64)],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    if let Some(row) = row {
        let meta_data: GdalMetaDataList = row.get(0);
        Ok(Some(meta_data))
    } else {
        Ok(None)
    }
}

async fn exists(path: &Path) -> Result<bool, NetCdfCf4DProviderError> {
    tokio::fs::try_exists(path)
        .await
        .boxed_context(error::UnexpectedExecution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        datasets::external::netcdfcf::{deferred_write_transaction, test_db_connection},
        tasks::util::NopTaskContext,
    };
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
            None,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn test_index_subdataset() {
        hide_gdal_errors();

        let dataset_in = format!(
            "NETCDF:\"{}\"",
            test_data!("netcdf4d/dataset_m.nc").display()
        );

        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.path().join("metric_1");

        let mut db = test_db_connection().await;
        let transaction = deferred_write_transaction(&mut db).await.unwrap();

        index_subdataset(
            Arc::new(ConversionMetadata {
                dataset_in,
                dataset_out_base: tempdir_path.clone(),
                array_path: "/metric_1/ebv_cube".to_string(),
                number_of_entities: 3,
                file_path: PathBuf::from("dataset_m.nc"),
                data_path: vec!["metric_1".to_string()],
            }),
            Arc::new(TimeCoverage {
                time_stamps: vec![
                    DateTime::new_utc(2000, 1, 1, 0, 0, 0).into(),
                    DateTime::new_utc(2001, 1, 1, 0, 0, 0).into(),
                    DateTime::new_utc(2002, 1, 1, 0, 0, 0).into(),
                ],
            }),
            None,
            &NopTaskContext,
            &mut Default::default(),
            (0, 1),
            &transaction,
        )
        .await
        .unwrap();

        for entity in 0..3 {
            for year in 2000..=2002 {
                let path = tempdir_path.join(format!("{entity}/{year}-01-01T00:00:00.000Z.tiff"));
                assert!(path.exists(), "Path {} does not exist", path.display());
            }
        }

        let sample_loading_info: GdalMetaDataList = meta_data_from_overviews(
            &transaction,
            "dataset_m.nc",
            vec!["metric_1".to_string()],
            1,
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
                            DateTime::new_utc(2000, 1, 1, 0, 0, 0),
                            DateTime::new_utc(2000, 1, 1, 0, 0, 0)
                        )
                        .unwrap(),
                        params: Some(GdalDatasetParameters {
                            file_path: tempdir_path.join("1/2000-01-01T00:00:00.000Z.tiff"),
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
                            DateTime::new_utc(2001, 1, 1, 0, 0, 0),
                            DateTime::new_utc(2001, 1, 1, 0, 0, 0)
                        )
                        .unwrap(),
                        params: Some(GdalDatasetParameters {
                            file_path: tempdir_path.join("1/2001-01-01T00:00:00.000Z.tiff"),
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
                            DateTime::new_utc(2002, 1, 1, 0, 0, 0),
                            DateTime::new_utc(2002, 1, 1, 0, 0, 0)
                        )
                        .unwrap(),
                        params: Some(GdalDatasetParameters {
                            file_path: tempdir_path.join("1/2002-01-01T00:00:00.000Z.tiff"),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_create_overviews() {
        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let mut db = test_db_connection().await;
        let transaction = deferred_write_transaction(&mut db).await.unwrap();

        create_overviews(
            test_data!("netcdf4d"),
            Path::new("dataset_m.nc"),
            overview_folder.path(),
            None,
            NopTaskContext,
            &transaction,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn test_create_overviews_irregular() {
        // crate::util::tests::initialize_debugging_in_test(); // TODO: remove

        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let mut db = test_db_connection().await;
        let transaction = deferred_write_transaction(&mut db).await.unwrap();

        create_overviews(
            test_data!("netcdf4d"),
            Path::new("dataset_irr_ts.nc"),
            overview_folder.path(),
            None,
            NopTaskContext,
            &transaction,
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

        let sample_loading_info: GdalMetaDataList = meta_data_from_overviews(
            &transaction,
            "dataset_irr_ts.nc",
            vec!["metric_2".to_string()],
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_remove_overviews() {
        fn is_empty(directory: &Path) -> bool {
            directory.read_dir().unwrap().next().is_none()
        }

        hide_gdal_errors();

        let overview_folder = tempfile::tempdir().unwrap();

        let dataset_path = Path::new("dataset_m.nc");

        let mut db = test_db_connection().await;
        let transaction = deferred_write_transaction(&mut db).await.unwrap();

        create_overviews(
            test_data!("netcdf4d"),
            dataset_path,
            overview_folder.path(),
            None,
            NopTaskContext,
            &transaction,
        )
        .await
        .unwrap();

        assert!(!is_empty(overview_folder.path()));

        remove_overviews(dataset_path, overview_folder.path(), &transaction, false)
            .await
            .unwrap();

        assert!(is_empty(overview_folder.path()));
    }
}
