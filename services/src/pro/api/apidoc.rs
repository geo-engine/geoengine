use super::handlers::permissions::{PermissionRequest, Resource};
use super::handlers::users::AddRole;
use super::model::MlModelId;
use crate::api::handlers;
use crate::api::handlers::plots::WrappedPlotOutput;
use crate::api::handlers::spatial_references::{AxisOrder, SpatialReferenceSpecification};
use crate::api::handlers::tasks::{TaskAbortOptions, TaskResponse};
use crate::api::handlers::upload::{UploadFileLayersResponse, UploadFilesResponse};
use crate::api::handlers::wfs::{CollectionType, GeoJson};
use crate::api::handlers::workflows::{ProvenanceEntry, RasterStreamWebsocketResultType};
use crate::api::model::datatypes::{
    AxisLabels, BoundingBox2D, Breakpoint, CacheTtlSeconds, ClassificationMeasurement, Colorizer,
    ContinuousMeasurement, Coordinate2D, DataId, DataProviderId, DatasetId, DateTime,
    DateTimeParseFormat, ExternalDataId, FeatureDataType, GdalConfigOption, LayerId,
    LinearGradient, LogarithmicGradient, Measurement, MultiLineString, MultiPoint, MultiPolygon,
    NamedData, NoGeometry, Palette, PlotOutputFormat, PlotQueryRectangle, RasterDataType,
    RasterPropertiesEntryType, RasterPropertiesKey, RasterQueryRectangle, RgbaColor,
    SpatialPartition2D, SpatialReferenceAuthority, SpatialResolution, StringPair, TimeGranularity,
    TimeInstance, TimeInterval, TimeStep, VectorDataType, VectorQueryRectangle,
};
use crate::api::model::operators::{
    CsvHeader, FileNotFoundHandling, FormatSpecifics, GdalDatasetGeoTransform,
    GdalDatasetParameters, GdalLoadingInfoTemporalSlice, GdalMetaDataList, GdalMetaDataRegular,
    GdalMetaDataStatic, GdalMetadataMapping, GdalMetadataNetCdfCf, GdalSourceTimePlaceholder,
    MockDatasetDataSourceLoadingInfo, MockMetaData, OgrMetaData, OgrSourceColumnSpec,
    OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec,
    OgrSourceTimeFormat, PlotResultDescriptor, RasterBandDescriptor, RasterBandDescriptors,
    RasterResultDescriptor, TimeReference, TypedGeometry, TypedOperator, TypedResultDescriptor,
    UnixTimeStampType, VectorColumnInfo, VectorResultDescriptor,
};
use crate::api::model::responses::datasets::DatasetNameResponse;
use crate::api::model::responses::{
    BadRequestQueryResponse, ErrorResponse, IdResponse, PayloadTooLargeResponse, PngResponse,
    UnauthorizedAdminResponse, UnauthorizedUserResponse, UnsupportedMediaTypeForJsonResponse,
    ZipResponse,
};
use crate::api::model::services::{
    AddDataset, CreateDataset, DataPath, DatasetDefinition, MetaDataDefinition, MetaDataSuggestion,
};
use crate::api::ogc::{util::OgcBoundingBox, wcs, wfs, wms};
use crate::contexts::SessionId;
use crate::datasets::listing::{DatasetListing, OrderBy, Provenance, ProvenanceOutput};
use crate::datasets::storage::{AutoCreateDataset, Dataset};
use crate::datasets::upload::{UploadId, Volume, VolumeName};
use crate::datasets::{DatasetName, RasterDatasetFromWorkflow, RasterDatasetFromWorkflowResult};
use crate::layers::layer::{
    AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollection, LayerCollectionListing,
    LayerListing, Property, ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::LayerCollectionId;
use crate::pro;
use crate::pro::api::handlers::users::{Quota, UpdateQuota};
use crate::pro::permissions::{Permission, ResourceId, Role, RoleDescription, RoleId};
use crate::pro::users::{UserCredentials, UserId, UserInfo, UserRegistration, UserSession};
use crate::projects::{
    ColorParam, CreateProject, DerivedColor, DerivedNumber, LayerUpdate, LayerVisibility,
    LineSymbology, NumberParam, Plot, PlotUpdate, PointSymbology, PolygonSymbology, Project,
    ProjectId, ProjectLayer, ProjectListing, ProjectVersion, ProjectVersionId, RasterSymbology,
    STRectangle, StrokeParam, Symbology, TextSymbology, UpdateProject,
};
use crate::tasks::{TaskFilter, TaskId, TaskListOptions, TaskStatus, TaskStatusWithId};
use crate::util::{
    apidoc::{OpenApiServerInfo, TransformSchemasWithTag},
    server::ServerInfo,
};
use crate::workflows::workflow::{Workflow, WorkflowId};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::util::server::available_handler,
        crate::util::server::server_info_handler,
        handlers::layers::layer_handler,
        handlers::layers::layer_to_workflow_id_handler,
        handlers::layers::list_collection_handler,
        handlers::layers::list_root_collections_handler,
        handlers::layers::add_layer,
        handlers::layers::add_collection,
        handlers::layers::remove_collection,
        handlers::layers::remove_layer_from_collection,
        handlers::layers::add_existing_layer_to_collection,
        handlers::layers::add_existing_collection_to_collection,
        handlers::layers::remove_collection_from_collection,
        handlers::layers::layer_to_dataset,
        handlers::tasks::abort_handler,
        handlers::tasks::list_handler,
        handlers::tasks::status_handler,
        handlers::wcs::wcs_capabilities_handler,
        handlers::wcs::wcs_describe_coverage_handler,
        handlers::wcs::wcs_get_coverage_handler,
        handlers::wfs::wfs_capabilities_handler,
        handlers::wfs::wfs_capabilities_handler,
        handlers::wfs::wfs_feature_handler,
        handlers::wms::wms_capabilities_handler,
        handlers::wms::wms_legend_graphic_handler,
        handlers::wms::wms_map_handler,
        handlers::workflows::dataset_from_workflow_handler,
        handlers::workflows::get_workflow_metadata_handler,
        handlers::workflows::get_workflow_provenance_handler,
        handlers::workflows::load_workflow_handler,
        handlers::workflows::raster_stream_websocket,
        handlers::workflows::register_workflow_handler,
        handlers::workflows::get_workflow_all_metadata_zip_handler,
        pro::api::handlers::users::anonymous_handler,
        pro::api::handlers::users::login_handler,
        pro::api::handlers::users::logout_handler,
        pro::api::handlers::users::quota_handler,
        pro::api::handlers::users::get_user_quota_handler,
        pro::api::handlers::users::update_user_quota_handler,
        pro::api::handlers::users::register_user_handler,
        pro::api::handlers::users::session_handler,
        pro::api::handlers::users::add_role_handler,
        pro::api::handlers::users::remove_role_handler,
        pro::api::handlers::users::assign_role_handler,
        pro::api::handlers::users::revoke_role_handler,
        pro::api::handlers::users::get_role_descriptions,
        handlers::datasets::delete_dataset_handler,
        handlers::datasets::list_datasets_handler,
        handlers::datasets::list_volumes_handler,
        handlers::datasets::get_dataset_handler,
        handlers::datasets::create_dataset_handler,
        handlers::datasets::auto_create_dataset_handler,
        handlers::datasets::suggest_meta_data_handler,
        handlers::spatial_references::get_spatial_reference_specification_handler,
        handlers::plots::get_plot_handler,
        handlers::projects::list_projects_handler,
        handlers::projects::project_versions_handler,
        handlers::projects::load_project_latest_handler,
        handlers::projects::create_project_handler,
        handlers::projects::update_project_handler,
        handlers::projects::delete_project_handler,
        handlers::projects::load_project_version_handler,
        handlers::upload::list_upload_files_handler,
        handlers::upload::list_upload_file_layers_handler,
        handlers::upload::upload_handler,
        pro::api::handlers::permissions::add_permission_handler,
        pro::api::handlers::permissions::remove_permission_handler
    ),
    components(
        responses(
            UnsupportedMediaTypeForJsonResponse,
            PayloadTooLargeResponse,
            IdResponse::<WorkflowId>,
            IdResponse::<UploadId>,
            IdResponse::<LayerId>,
            IdResponse::<LayerCollectionId>,
            IdResponse::<ProjectId>,
            DatasetNameResponse,
            UnauthorizedAdminResponse,
            UnauthorizedUserResponse,
            BadRequestQueryResponse,
            PngResponse,
            ZipResponse
        ),
        schemas(
            ErrorResponse,
            UserSession,
            UserCredentials,
            UserRegistration,
            DateTime,
            UserInfo,
            Quota,
            UpdateQuota,

            DataId,
            DataProviderId,
            DatasetId,
            DatasetName,
            NamedData,
            ExternalDataId,
            LayerId,
            ProjectId,
            RoleId,
            SessionId,
            TaskId,
            UploadId,
            UserId,
            WorkflowId,
            ProviderLayerId,
            ProviderLayerCollectionId,
            LayerCollectionId,
            ProjectVersionId,

            TimeInstance,
            TimeInterval,

            Coordinate2D,
            BoundingBox2D,
            SpatialPartition2D,
            SpatialResolution,
            SpatialReferenceAuthority,
            SpatialReferenceSpecification,
            AxisOrder,
            Measurement,
            ContinuousMeasurement,
            ClassificationMeasurement,
            STRectangle,

            ProvenanceEntry,
            ProvenanceOutput,
            Provenance,

            VectorDataType,
            FeatureDataType,
            RasterDataType,

            ServerInfo,

            Workflow,
            TypedOperator,
            TypedResultDescriptor,
            PlotResultDescriptor,
            RasterResultDescriptor,
            RasterBandDescriptor,
            RasterBandDescriptors,
            VectorResultDescriptor,
            VectorColumnInfo,
            RasterDatasetFromWorkflow,
            RasterDatasetFromWorkflowResult,
            RasterQueryRectangle,
            VectorQueryRectangle,
            PlotQueryRectangle,

            TaskAbortOptions,
            TaskFilter,
            TaskListOptions,
            TaskStatus,
            TaskStatusWithId,
            TaskResponse,

            Layer,
            LayerListing,
            LayerCollection,
            LayerCollectionListing,
            Property,
            CollectionItem,
            AddLayer,
            AddLayerCollection,

            Breakpoint,
            ColorParam,
            Colorizer,
            DerivedColor,
            DerivedNumber,
            LineSymbology,
            NumberParam,
            Palette,
            PointSymbology,
            PolygonSymbology,
            RasterSymbology,
            RgbaColor,
            StrokeParam,
            Symbology,
            TextSymbology,
            LinearGradient,
            LogarithmicGradient,

            OgcBoundingBox,

            wcs::request::WcsService,
            wcs::request::WcsVersion,
            wcs::request::GetCapabilitiesRequest,
            wcs::request::DescribeCoverageRequest,
            wcs::request::GetCoverageRequest,
            wcs::request::GetCoverageFormat,
            wcs::request::WcsBoundingbox,

            wms::request::WmsService,
            wms::request::WmsVersion,
            wms::request::GetCapabilitiesFormat,
            wms::request::GetCapabilitiesRequest,
            wms::request::GetMapRequest,
            wms::request::GetMapExceptionFormat,
            wms::request::GetMapFormat,
            wms::request::GetLegendGraphicRequest,

            wfs::request::WfsService,
            wfs::request::WfsVersion,
            wfs::request::GetCapabilitiesRequest,
            wfs::request::WfsResolution,
            wfs::request::GetFeatureRequest,
            wfs::request::TypeNames,

            GeoJson,
            CollectionType,

            UploadFilesResponse,
            UploadFileLayersResponse,

            CreateDataset,
            AutoCreateDataset,
            OrderBy,
            DatasetListing,
            MetaDataSuggestion,
            MetaDataDefinition,
            MockMetaData,
            GdalMetaDataRegular,
            GdalMetaDataStatic,
            GdalMetadataNetCdfCf,
            GdalMetaDataList,
            GdalDatasetParameters,
            TimeStep,
            GdalSourceTimePlaceholder,
            GdalLoadingInfoTemporalSlice,
            FileNotFoundHandling,
            GdalDatasetGeoTransform,
            GdalMetadataMapping,
            TimeGranularity,
            DateTimeParseFormat,
            TimeReference,
            RasterPropertiesKey,
            RasterPropertiesEntryType,
            OgrMetaData,
            StringPair,
            GdalConfigOption,
            AxisLabels,
            MockDatasetDataSourceLoadingInfo,
            OgrSourceDataset,
            OgrSourceColumnSpec,
            TypedGeometry,
            OgrSourceErrorSpec,
            OgrSourceDatasetTimeType,
            OgrSourceDurationSpec,
            OgrSourceTimeFormat,
            NoGeometry,
            MultiPoint,
            MultiLineString,
            MultiPolygon,
            FormatSpecifics,
            CsvHeader,
            UnixTimeStampType,
            Dataset,
            DatasetDefinition,
            AddDataset,
            Volume,
            VolumeName,
            DataPath,

            PlotOutputFormat,
            WrappedPlotOutput,

            CreateProject,
            Project,
            UpdateProject,
            ProjectListing,
            ProjectVersion,
            LayerUpdate,
            PlotUpdate,
            Plot,
            ProjectLayer,
            LayerVisibility,
            RasterStreamWebsocketResultType,
            CacheTtlSeconds,

            PermissionRequest,
            Resource,
            ResourceId,
            Permission,
            AddRole,
            RoleDescription,
            Role,

            MlModelId
        ),
    ),
    modifiers(&SecurityAddon, &ApiDocInfo, &OpenApiServerInfo, &TransformSchemasWithTag),
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
                    "https://github.com/geo-engine/geoengine/blob/main/LICENSE",
                ))
                .build(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pro::util::tests::send_pro_test_request;
    use crate::pro::util::tests::with_pro_temp_context;

    #[test]
    fn can_resolve_api() {
        crate::util::openapi_examples::can_resolve_api(ApiDoc::openapi());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn can_run_examples() {
        with_pro_temp_context(|app_ctx, _| async move {
            crate::pro::util::openapi_examples::can_run_pro_examples(
                app_ctx,
                ApiDoc::openapi(),
                send_pro_test_request,
            )
            .await;
        })
        .await;
    }
}
