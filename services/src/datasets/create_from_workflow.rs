use crate::api::model::datatypes::RasterQueryRectangle;
use crate::api::model::services::AddDataset;
use crate::contexts::SessionContext;
use crate::datasets::listing::DatasetProvider;
use crate::datasets::storage::{DatasetDefinition, DatasetStore, MetaDataDefinition};
use crate::datasets::upload::{UploadId, UploadRootPath};
use crate::error;
use crate::tasks::{Task, TaskId, TaskManager, TaskStatusInfo};
use float_cmp::approx_eq;
use geoengine_datatypes::error::ErrorSource;
use geoengine_datatypes::primitives::{BandSelection, TimeInterval};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::call_on_generic_raster_processor_gdal_types;
use geoengine_operators::engine::{
    ExecutionContext, InitializedRasterOperator, QueryContext, RasterResultDescriptor,
};
use geoengine_operators::source::{
    GdalLoadingInfoTemporalSlice, GdalMetaDataList, GdalMetaDataStatic,
};
use geoengine_operators::util::raster_stream_to_geotiff::{
    raster_stream_to_geotiff, GdalCompressionNumThreads, GdalGeoTiffDatasetMetadata,
    GdalGeoTiffOptions,
};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use utoipa::ToSchema;

use super::{DatasetIdAndName, DatasetName};

/// parameter for the dataset from workflow handler (body)
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(example = json!({"name": "foo", "displayName": "a new dataset", "description": null, "query": {"spatialBounds": {"upperLeftCoordinate": {"x": -10.0, "y": 80.0}, "lowerRightCoordinate": {"x": 50.0, "y": 20.0}}, "timeInterval": {"start": 1_388_534_400_000_i64, "end": 1_388_534_401_000_i64}, "spatialResolution": {"x": 0.1, "y": 0.1}}}))]
#[serde(rename_all = "camelCase")]
pub struct RasterDatasetFromWorkflow {
    pub name: Option<DatasetName>,
    pub display_name: String,
    pub description: Option<String>,
    pub query: RasterQueryRectangle,
    #[schema(default = default_as_cog)]
    #[serde(default = "default_as_cog")]
    pub as_cog: bool,
}

pub struct RasterDatasetFromWorkflowParams {
    pub name: Option<DatasetName>,
    pub display_name: String,
    pub description: Option<String>,
    pub query: geoengine_datatypes::primitives::RasterQueryRectangle,
    pub as_cog: bool,
}

impl RasterDatasetFromWorkflowParams {
    pub fn from_request_and_result_descriptor(
        request: RasterDatasetFromWorkflow,
        result_descriptor: &RasterResultDescriptor,
        tiling_spec: TilingSpecification,
    ) -> error::Result<Self> {
        let query = request.query;

        // FIXME: handle resolutions
        // TODO: allow to use pixel bounds in query?
        ensure!(
            approx_eq!(
                f64,
                result_descriptor
                    .spatial_grid_descriptor()
                    .spatial_resolution()
                    .x,
                query.spatial_resolution.x
            ) && approx_eq!(
                f64,
                result_descriptor
                    .spatial_grid_descriptor()
                    .spatial_resolution()
                    .y,
                query.spatial_resolution.y
            ),
            error::ResolutionMissmatch,
        );

        let grid_bounds = result_descriptor
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec)
            .tiling_geo_transform()
            .spatial_to_grid_bounds(&query.spatial_bounds.into()); // TODO: somehow clean up api and inner structs

        let raster_query =
            geoengine_datatypes::primitives::RasterQueryRectangle::new_with_grid_bounds(
                grid_bounds,
                query.time_interval.into(),
                BandSelection::first_n(result_descriptor.bands.len() as u32 + 1), // FIXME: what to do here?
            );

        Ok(Self {
            name: request.name,
            display_name: request.display_name,
            description: request.description,
            query: raster_query,
            as_cog: request.as_cog,
        })
    }
}

/// By default, we set [`RasterDatasetFromWorkflow::as_cog`] to true to produce cloud-optmized `GeoTiff`s.
#[inline]
const fn default_as_cog() -> bool {
    true
}

/// response of the dataset from workflow handler
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct RasterDatasetFromWorkflowResult {
    pub dataset: DatasetName,
    pub upload: UploadId,
}

impl TaskStatusInfo for RasterDatasetFromWorkflowResult {}

pub struct RasterDatasetFromWorkflowTask<C: SessionContext, R: InitializedRasterOperator> {
    pub source_name: String,
    pub initialized_operator: R,
    pub ctx: Arc<C>,
    pub info: RasterDatasetFromWorkflowParams,
    pub upload: UploadId,
    pub file_path: PathBuf,
    pub compression_num_threads: GdalCompressionNumThreads,
}

impl<C: SessionContext, R: InitializedRasterOperator> RasterDatasetFromWorkflowTask<C, R> {
    async fn process(&self) -> error::Result<RasterDatasetFromWorkflowResult> {
        let result_descriptor = self.initialized_operator.result_descriptor();

        let processor = self
            .initialized_operator
            .query_processor()
            .context(crate::error::Operator)?;

        let query_rect: &geoengine_datatypes::primitives::QueryRectangle<
            geoengine_datatypes::primitives::SpatialGridQueryRectangle,
            BandSelection,
        > = &self.info.query;

        let query_ctx = self.ctx.query_context()?;
        let request_spatial_ref =
            Option::<SpatialReference>::from(result_descriptor.spatial_reference)
                .ok_or(crate::error::Error::MissingSpatialReference)?;
        let tile_limit = None; // TODO: set a reasonable limit or make configurable?

        let tiling_strat = result_descriptor
            .spatial_grid_descriptor()
            .tiling_grid_definition(query_ctx.tiling_specification())
            .generate_data_tiling_strategy();

        // build the geotiff
        let res =
            call_on_generic_raster_processor_gdal_types!(processor, p => raster_stream_to_geotiff(
            &self.file_path,
            p,
            query_rect.clone().into(), // FIXME: unnecessary clone
            query_ctx,
            GdalGeoTiffDatasetMetadata {
                no_data_value: Default::default(), // TODO: decide how to handle the no data here
                spatial_reference: request_spatial_ref,
            },
            GdalGeoTiffOptions {
                compression_num_threads: self.compression_num_threads,
                as_cog: self.info.as_cog,
                force_big_tiff: false,
            },
            tile_limit,
            Box::pin(futures::future::pending()), // datasets shall continue to be built in the background and not cancelled
            tiling_strat,
        ).await)?
            .map_err(crate::error::Error::from)?;

        // create the dataset
        let dataset = create_dataset(
            &self.info,
            res,
            result_descriptor,
            query_rect,
            self.ctx.as_ref(),
        )
        .await?;

        Ok(RasterDatasetFromWorkflowResult {
            dataset: dataset.name,
            upload: self.upload,
        })
    }
}

#[async_trait::async_trait]
impl<C: SessionContext, R: InitializedRasterOperator> Task<C::TaskContext>
    for RasterDatasetFromWorkflowTask<C, R>
{
    async fn run(
        &self,
        _ctx: C::TaskContext,
    ) -> error::Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let response = self.process().await;

        response
            .map(TaskStatusInfo::boxed)
            .map_err(ErrorSource::boxed)
    }

    async fn cleanup_on_error(
        &self,
        _ctx: C::TaskContext,
    ) -> error::Result<(), Box<dyn ErrorSource>> {
        fs::remove_dir_all(&self.file_path)
            .await
            .context(crate::error::Io)
            .map_err(ErrorSource::boxed)?;

        //TODO: Dataset might already be in the database, if task was already close to finishing.

        Ok(())
    }

    fn task_type(&self) -> &'static str {
        "create-dataset"
    }

    fn task_unique_id(&self) -> Option<String> {
        Some(self.upload.to_string())
    }

    fn task_description(&self) -> String {
        format!(
            "Creating dataset {} from {}",
            self.info.display_name, self.source_name
        )
    }
}

pub async fn schedule_raster_dataset_from_workflow_task<
    C: SessionContext,
    R: InitializedRasterOperator + 'static,
>(
    source_name: String,
    initialized_operator: R,
    ctx: Arc<C>,
    info: RasterDatasetFromWorkflowParams,
    compression_num_threads: GdalCompressionNumThreads,
) -> error::Result<TaskId> {
    if let Some(dataset_name) = &info.name {
        let db = ctx.db();

        // try to resolve the dataset name to an id
        let potential_id_result = db.resolve_dataset_name_to_id(dataset_name).await?;

        // handle the case where the dataset name is already taken
        if let Some(dataset_id) = potential_id_result {
            return Err(error::Error::DatasetNameAlreadyExists {
                dataset_name: dataset_name.to_string(),
                dataset_id: dataset_id.into(),
            });
        }
    }

    let upload = UploadId::new();
    let upload_path = upload.root_path()?;
    fs::create_dir_all(&upload_path)
        .await
        .context(crate::error::Io)?;
    let file_path = upload_path.clone();

    let task = RasterDatasetFromWorkflowTask {
        source_name,
        initialized_operator,
        ctx: ctx.clone(),
        info,
        upload,
        file_path,
        compression_num_threads,
    }
    .boxed();

    let task_id = ctx.tasks().schedule_task(task, None).await?;

    Ok(task_id)
}

async fn create_dataset<C: SessionContext>(
    info: &RasterDatasetFromWorkflowParams,
    mut slice_info: Vec<GdalLoadingInfoTemporalSlice>,
    origin_result_descriptor: &RasterResultDescriptor,
    query_rectangle: &geoengine_datatypes::primitives::RasterQueryRectangle,
    ctx: &C,
) -> error::Result<DatasetIdAndName> {
    ensure!(!slice_info.is_empty(), error::EmptyDatasetCannotBeImported);

    let first_start = slice_info
        .first()
        .expect("slice_info should have at least one element")
        .time
        .start();
    let last_end = slice_info
        .last()
        .expect("slice_info should have at least one element")
        .time
        .end();
    let result_time_interval = TimeInterval::new(first_start, last_end)?;

    let exe_ctx = ctx.execution_context()?;

    let source_tiling_spatial_grid =
        origin_result_descriptor.tiling_grid_definition(exe_ctx.tiling_specification());
    let query_tiling_spatial_grid =
        source_tiling_spatial_grid.with_other_bounds(query_rectangle.spatial_query.grid_bounds());
    let result_descriptor_bounds = origin_result_descriptor
        .spatial_grid_descriptor()
        .intersection_with_tiling_grid(&query_tiling_spatial_grid)
        .ok_or(error::Error::EmptyDatasetCannotBeImported)?; // TODO: maybe allow empty datasets?

    let result_descriptor = RasterResultDescriptor {
        data_type: origin_result_descriptor.data_type,
        spatial_reference: origin_result_descriptor.spatial_reference,
        time: Some(result_time_interval),
        spatial_grid: result_descriptor_bounds,
        bands: origin_result_descriptor.bands.clone(),
    };
    //TODO: Recognize MetaDataDefinition::GdalMetaDataRegular
    let meta_data = if slice_info.len() == 1 {
        let loading_info_slice = slice_info.pop().expect("slice_info has len one");
        let time = Some(loading_info_slice.time);
        let params = loading_info_slice
            .params
            .expect("datasets with exactly one timestep should have data");
        let cache_ttl = loading_info_slice.cache_ttl;
        MetaDataDefinition::GdalStatic(GdalMetaDataStatic {
            time,
            params,
            result_descriptor,
            cache_ttl,
        })
    } else {
        MetaDataDefinition::GdalMetaDataList(GdalMetaDataList {
            result_descriptor,
            params: slice_info,
        })
    };

    let dataset_definition = DatasetDefinition {
        properties: AddDataset {
            name: info.name.clone(),
            display_name: info.display_name.clone(),
            description: info.description.clone().unwrap_or_default(),
            source_operator: "GdalSource".to_owned(),
            symbology: None,  // TODO add symbology?
            provenance: None, // TODO add provenance that references the workflow
            tags: Some(vec!["workflow".to_owned()]),
        }
        .into(),
        meta_data,
    };

    let db = ctx.db();
    let result = db
        .add_dataset(dataset_definition.properties, dataset_definition.meta_data)
        .await?;

    Ok(result)
}
