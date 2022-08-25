use crate::datasets::listing::{Provenance, ProvenanceOutput};
use crate::datasets::upload::UploadId;
use crate::handlers;
use crate::handlers::workflows::{RasterDatasetFromWorkflow, RasterDatasetFromWorkflowResult};
use crate::server::VersionInfo;
use crate::util::IdResponse;
use crate::workflows::workflow::{Workflow, WorkflowId};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DataId, DataProviderId, DatasetId, ExternalDataId, LayerId};
use geoengine_datatypes::primitives::{
    BoundingBox2D, ClassificationMeasurement, ContinuousMeasurement, Coordinate2D, Measurement,
    PlotQueryRectangle, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInstance,
    TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::{
    SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption,
};
use geoengine_operators::engine::{
    PlotResultDescriptor, RasterResultDescriptor, TypedOperator, TypedResultDescriptor,
    VectorResultDescriptor,
};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

#[derive(OpenApi)]
#[openapi(
    handlers(
        crate::server::show_version_handler,
        handlers::workflows::register_workflow_handler,
        handlers::workflows::load_workflow_handler,
        handlers::workflows::get_workflow_metadata_handler,
        handlers::workflows::get_workflow_provenance_handler,
        handlers::workflows::dataset_from_workflow_handler,
    ),
    components(
        DataId,
        DataProviderId,
        DatasetId,
        ExternalDataId,
        IdResponse<WorkflowId>,
        LayerId,
        UploadId,
        WorkflowId,

        TimeInstance,
        TimeInterval,

        Coordinate2D,
        BoundingBox2D,
        SpatialPartition2D,
        SpatialResolution,
        SpatialReference,
        SpatialReferenceOption,
        SpatialReferenceAuthority,
        Measurement,
        ContinuousMeasurement,
        ClassificationMeasurement,

        ProvenanceOutput,
        Provenance,

        VectorDataType,
        RasterDataType,

        VersionInfo,

        Workflow,
        TypedOperator,
        TypedResultDescriptor,
        PlotResultDescriptor,
        RasterResultDescriptor,
        VectorResultDescriptor,
        RasterDatasetFromWorkflow,
        RasterDatasetFromWorkflowResult,
        RasterQueryRectangle,
        VectorQueryRectangle,
        PlotQueryRectangle,
    ),
    modifiers(&SecurityAddon),
    external_docs(url = "https://docs.geoengine.io", description = "Geo Engine Docs")
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.as_mut().unwrap();
        components.add_security_scheme(
            "session_token",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("UUID")
                    .build(),
            ),
        );
    }
}
