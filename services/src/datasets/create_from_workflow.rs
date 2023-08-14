use crate::api::model::datatypes::{DatasetName, RasterQueryRectangle};
use crate::api::model::responses::datasets::DatasetIdAndName;
use crate::api::model::services::AddDataset;
use crate::contexts::SessionContext;
use crate::datasets::storage::{DatasetDefinition, DatasetStore, MetaDataDefinition};
use crate::datasets::upload::{UploadId, UploadRootPath};
use crate::error;
use crate::tasks::{Task, TaskId, TaskManager, TaskStatusInfo};
use crate::workflows::workflow::Workflow;
use geoengine_datatypes::error::ErrorSource;
use geoengine_datatypes::primitives::{SpatialPartitioned, TimeInterval};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::call_on_generic_raster_processor_gdal_types;
use geoengine_operators::engine::{
    ExecutionContext, InitializedRasterOperator, RasterResultDescriptor, WorkflowOperatorPath,
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

pub struct RasterDatasetFromWorkflowTask<C: SessionContext> {
    pub workflow: Workflow,
    pub ctx: Arc<C>,
    pub info: RasterDatasetFromWorkflow,
    pub upload: UploadId,
    pub file_path: PathBuf,
    pub compression_num_threads: GdalCompressionNumThreads,
}

impl<C: SessionContext> RasterDatasetFromWorkflowTask<C> {
    async fn process(&self) -> error::Result<RasterDatasetFromWorkflowResult> {
        let operator = self.workflow.operator.clone();

        let operator = operator.get_raster().context(crate::error::Operator)?;

        let execution_context = self.ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = operator
            .initialize(workflow_operator_path_root, &execution_context)
            .await
            .context(crate::error::Operator)?;

        let result_descriptor = initialized.result_descriptor();

        let processor = initialized
            .query_processor()
            .context(crate::error::Operator)?;

        let api_query_rect = self.info.query;
        let query_rect = geoengine_datatypes::primitives::RasterQueryRectangle::with_partition_and_resolution_and_origin(
            api_query_rect.spatial_bounds.into(),
            api_query_rect.spatial_resolution.into(),
            execution_context.tiling_specification().origin_coordinate,
            api_query_rect.time_interval.into(),
        );

        let query_ctx = self.ctx.query_context()?;
        let request_spatial_ref =
            Option::<SpatialReference>::from(result_descriptor.spatial_reference)
                .ok_or(crate::error::Error::MissingSpatialReference)?;
        let tile_limit = None; // TODO: set a reasonable limit or make configurable?

        // build the geotiff
        let res =
            call_on_generic_raster_processor_gdal_types!(processor, p => raster_stream_to_geotiff(
            &self.file_path,
            p,
            query_rect,
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
            execution_context.tiling_specification(),
        ).await)?
            .map_err(crate::error::Error::from)?;

        // create the dataset
        let dataset = create_dataset(
            self.info.clone(),
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
impl<C: SessionContext> Task<C::TaskContext> for RasterDatasetFromWorkflowTask<C> {
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
}

pub async fn schedule_raster_dataset_from_workflow_task<C: SessionContext>(
    workflow: Workflow,
    ctx: Arc<C>,
    info: RasterDatasetFromWorkflow,
    compression_num_threads: GdalCompressionNumThreads,
) -> error::Result<TaskId> {
    let upload = UploadId::new();
    let upload_path = upload.root_path()?;
    fs::create_dir_all(&upload_path)
        .await
        .context(crate::error::Io)?;
    let file_path = upload_path.clone();

    let task = RasterDatasetFromWorkflowTask {
        workflow,
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
    info: RasterDatasetFromWorkflow,
    mut slice_info: Vec<GdalLoadingInfoTemporalSlice>,
    origin_result_descriptor: &RasterResultDescriptor,
    query_rectangle: geoengine_datatypes::primitives::RasterQueryRectangle,
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

    let result_descriptor = RasterResultDescriptor {
        data_type: origin_result_descriptor.data_type,
        spatial_reference: origin_result_descriptor.spatial_reference,
        measurement: origin_result_descriptor.measurement.clone(),
        time: Some(result_time_interval),
        bbox: Some(query_rectangle.spatial_query().spatial_partition()),
        resolution: Some(query_rectangle.spatial_query().spatial_resolution()),
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
            name: info.name,
            display_name: info.display_name,
            description: info.description.unwrap_or_default(),
            source_operator: "GdalSource".to_owned(),
            symbology: None,  // TODO add symbology?
            provenance: None, // TODO add provenance that references the workflow
        },
        meta_data,
    };

    let db = ctx.db();
    let meta = db.wrap_meta_data(dataset_definition.meta_data);
    let result = db.add_dataset(dataset_definition.properties, meta).await?;

    Ok(result)
}
