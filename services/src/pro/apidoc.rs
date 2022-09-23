use crate::api::model::datatypes::{
    BoundingBox2D, ClassificationMeasurement, ContinuousMeasurement, Coordinate2D, DataId,
    DataProviderId, DatasetId, DateTime, ExternalDataId, FeatureDataType, LayerId, Measurement,
    RasterDataType, RasterQueryRectangle, SpatialPartition2D, SpatialReference,
    SpatialReferenceAuthority, SpatialReferenceOption, SpatialResolution, TimeInstance,
    TimeInterval, VectorDataType,
};
use crate::api::model::operators::{
    PlotResultDescriptor, RasterResultDescriptor, TypedOperator, TypedResultDescriptor,
    VectorColumnInfo, VectorResultDescriptor,
};
use crate::contexts::SessionId;
use crate::datasets::listing::{Provenance, ProvenanceOutput};
use crate::datasets::upload::UploadId;
use crate::handlers;
use crate::handlers::tasks::TaskAbortOptions;
use crate::handlers::workflows::{RasterDatasetFromWorkflow, RasterDatasetFromWorkflowResult};
use crate::pro;
use crate::projects::{ProjectId, STRectangle};
use crate::tasks::{TaskFilter, TaskId, TaskListOptions, TaskStatus};
use crate::util::server::VersionInfo;
use crate::util::{apidoc::ServerInfo, IdResponse};
use crate::workflows::workflow::{Workflow, WorkflowId};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

use super::datasets::RoleId;
use super::users::{UserCredentials, UserId, UserInfo, UserRegistration, UserSession};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::util::server::show_version_handler,
        handlers::tasks::abort_handler,
        handlers::tasks::list_handler,
        handlers::tasks::status_handler,
        handlers::workflows::dataset_from_workflow_handler,
        handlers::workflows::get_workflow_metadata_handler,
        handlers::workflows::get_workflow_provenance_handler,
        handlers::workflows::load_workflow_handler,
        handlers::workflows::register_workflow_handler,
        pro::handlers::users::anonymous_handler,
        pro::handlers::users::login_handler,
        pro::handlers::users::logout_handler,
        pro::handlers::users::register_user_handler,
        pro::handlers::users::session_handler,
    ),
    components(
        schemas(
            UserSession,
            UserCredentials,
            UserRegistration,
            DateTime,
            UserInfo,

            DataId,
            DataProviderId,
            DatasetId,
            ExternalDataId,
            IdResponse<WorkflowId>,
            LayerId,
            ProjectId,
            RoleId,
            SessionId,
            TaskId,
            UploadId,
            UserId,
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
            STRectangle,

            ProvenanceOutput,
            Provenance,

            VectorDataType,
            FeatureDataType,
            RasterDataType,

            VersionInfo,

            Workflow,
            TypedOperator,
            TypedResultDescriptor,
            PlotResultDescriptor,
            RasterResultDescriptor,
            VectorResultDescriptor,
            VectorColumnInfo,
            RasterDatasetFromWorkflow,
            RasterDatasetFromWorkflowResult,
            RasterQueryRectangle,
            // VectorQueryRectangle,
            // PlotQueryRectangle,

            TaskAbortOptions,
            TaskFilter,
            TaskListOptions,
            TaskStatus,
        ),
    ),
    modifiers(&SecurityAddon, &ApiDocInfo, &ServerInfo),
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
                    .description(Some("A valid session token can be obtained via the /anonymous or /login (pro only) endpoints. Alternatively, it can be defined as a fixed value in the Settings.toml file."))
                    .build(),
            ),
        );
    }
}

struct ApiDocInfo;

impl Modify for ApiDocInfo {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi.info.title = "Geo Engine Pro API".to_string();

        openapi.info.contact = Some(
            utoipa::openapi::ContactBuilder::new()
                .name(Some("Geo Engine Developers"))
                .email(Some("dev@geoengine.de"))
                .build(),
        );

        openapi.info.license = Some(
            utoipa::openapi::LicenseBuilder::new()
                .name("Apache 2.0 (pro features excluded)")
                .url(Some(
                    "https://github.com/geo-engine/geoengine/blob/master/LICENSE",
                ))
                .build(),
        );
    }
}
