use crate::api::model::datatypes::RasterToDatasetQueryRectangle;
use crate::api::model::services::AddDataset;
use crate::contexts::SessionContext;
use crate::datasets::listing::DatasetProvider;
use crate::datasets::storage::{DatasetDefinition, DatasetStore, MetaDataDefinition};
use crate::datasets::upload::{UploadId, UploadRootPath};
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use crate::{
    error,
    tasks::{Task, TaskId, TaskManager, TaskStatusInfo},
};
use geoengine_datatypes::error::ErrorSource;
use geoengine_datatypes::primitives::{BandSelection, TimeInterval};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::call_on_generic_raster_processor_gdal_types;
use geoengine_operators::engine::{
    ExecutionContext, InitializedRasterOperator, RasterResultDescriptor, TimeDescriptor,
    WorkflowOperatorPath,
};
use geoengine_operators::source::{
    GdalLoadingInfoTemporalSlice, GdalMetaDataList, GdalMetaDataStatic,
};
use geoengine_operators::util::raster_stream_to_geotiff::{
    GdalCompressionNumThreads, GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
    raster_stream_to_geotiff,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use utoipa::ToSchema;
use uuid::Uuid;

use super::{DatasetIdAndName, DatasetName};

/// parameter for the dataset from workflow handler (body)
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(example = json!({"name": "foo", "displayName": "a new dataset", "description": null, "query": {"spatialBounds": {"upperLeftCoordinate": {"x": -10.0, "y": 80.0}, "lowerRightCoordinate": {"x": 50.0, "y": 20.0}}, "timeInterval": {"start": 1_388_534_400_000_i64, "end": 1_388_534_401_000_i64}}}))]
#[serde(rename_all = "camelCase")]
pub struct RasterDatasetFromWorkflow {
    pub name: Option<DatasetName>,
    pub display_name: String,
    pub description: Option<String>,
    pub query: RasterToDatasetQueryRectangle,
    #[schema(default = default_as_cog)]
    #[serde(default = "default_as_cog")]
    pub as_cog: bool,
}

pub struct RasterDatasetFromWorkflowParams {
    pub name: Option<DatasetName>,
    pub display_name: String,
    pub description: Option<String>,
    pub query: Option<RasterToDatasetQueryRectangle>,
    pub as_cog: bool,
}

impl RasterDatasetFromWorkflowParams {
    pub fn from_request_and_result_descriptor(request: RasterDatasetFromWorkflow) -> Self {
        Self {
            name: request.name,
            display_name: request.display_name,
            description: request.description,
            query: Some(request.query),
            as_cog: request.as_cog,
        }
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

pub struct RasterDatasetFromWorkflowTask<C: SessionContext> {
    pub source_name: String,

    pub workflow_id: WorkflowId,
    pub ctx: Arc<C>,
    pub info: RasterDatasetFromWorkflowParams,
    pub upload: UploadId,
    pub file_path: PathBuf,
    pub compression_num_threads: GdalCompressionNumThreads,
}

impl<C: SessionContext> RasterDatasetFromWorkflowTask<C> {
    async fn process(&self) -> error::Result<RasterDatasetFromWorkflowResult> {
        let workflow = self.ctx.db().load_workflow(&self.workflow_id).await?;
        let exe_ctx = self.ctx.execution_context()?;

        let initialized_operator = workflow
            .clone()
            .operator
            .get_raster()
            .expect("must be raster here")
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await?;

        let tiling_spec = exe_ctx.tiling_specification();
        let result_descriptor = initialized_operator.result_descriptor();

        let query_rect = if let Some(sq) = self.info.query {
            let grid_bounds = result_descriptor
                .spatial_grid_descriptor()
                .tiling_grid_definition(tiling_spec)
                .tiling_geo_transform()
                .spatial_to_grid_bounds(&sq.spatial_bounds.into());

            geoengine_datatypes::primitives::RasterQueryRectangle::new(
                grid_bounds,
                sq.time_interval.into(),
                BandSelection::first_n(result_descriptor.bands.len() as u32),
            )
        } else {
            let grid_bounds = result_descriptor
                .tiling_grid_definition(tiling_spec)
                .tiling_grid_bounds();

            let qt = result_descriptor.time.bounds.ok_or(
                crate::error::Error::LayerResultDescriptorMissingFields {
                    field: "time".to_string(),
                    cause: "is None".to_string(),
                },
            )?;

            geoengine_datatypes::primitives::RasterQueryRectangle::new(
                grid_bounds,
                qt, // TODO: is this a good default?
                BandSelection::first_n(result_descriptor.bands.len() as u32),
            )
        };

        let query_ctx = self.ctx.query_context(self.workflow_id.0, Uuid::new_v4())?;
        let request_spatial_ref =
            Option::<SpatialReference>::from(result_descriptor.spatial_reference)
                .ok_or(crate::error::Error::MissingSpatialReference)?;
        let tile_limit = None; // TODO: set a reasonable limit or make configurable?

        let processor = initialized_operator.query_processor()?;

        // build the geotiff
        let res =
            call_on_generic_raster_processor_gdal_types!(processor, p => raster_stream_to_geotiff(
            &self.file_path,
            p,
            query_rect.clone(),
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
        ).await)?
            .map_err(crate::error::Error::from)?;
        // create the dataset
        let dataset = create_dataset(
            &self.info,
            res,
            result_descriptor,
            &query_rect,
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

    fn task_description(&self) -> String {
        format!(
            "Creating dataset {} from {}",
            self.info.display_name, self.source_name
        )
    }
}

pub async fn schedule_raster_dataset_from_workflow_task<C: SessionContext>(
    source_name: String,
    workflow_id: WorkflowId,
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
        workflow_id,
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
        source_tiling_spatial_grid.with_other_bounds(query_rectangle.spatial_bounds());
    let result_descriptor_bounds = origin_result_descriptor
        .spatial_grid_descriptor()
        .intersection_with_tiling_grid(&query_tiling_spatial_grid)
        .ok_or(error::Error::EmptyDatasetCannotBeImported)?; // TODO: maybe allow empty datasets?

    // TODO: this is not how it is intended to work with the spatial grid descriptor. The source should propably not need that defined in its params since it can be derived from the dataset!
    let (_state, dataset_source_descriptor_spatial_grid) = result_descriptor_bounds.as_parts();

    let dataset_spatial_grid = geoengine_operators::engine::SpatialGridDescriptor::new_source(
        dataset_source_descriptor_spatial_grid,
    );

    let result_descriptor = RasterResultDescriptor {
        data_type: origin_result_descriptor.data_type,
        spatial_reference: origin_result_descriptor.spatial_reference,
        time: TimeDescriptor::new(
            Some(result_time_interval),
            origin_result_descriptor.time.dimension,
        ),
        spatial_grid: dataset_spatial_grid,
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
