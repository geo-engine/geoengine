use crate::{
    api::{
        handlers::{
            self,
            datasets::VolumeFileLayersResponse,
            permissions::{PermissionListing, PermissionRequest, Resource},
            plots::WrappedPlotOutput,
            spatial_references::{AxisOrder, SpatialReferenceSpecification},
            tasks::TaskResponse,
            upload::{UploadFileLayersResponse, UploadFilesResponse},
            users::{AddRole, Quota, UpdateQuota, UsageSummaryGranularity},
            wfs::{CollectionType, GeoJson},
            workflows::{ProvenanceEntry, RasterStreamWebsocketResultType},
        },
        model::{
            datatypes::{
                AxisLabels, BoundingBox2D, Breakpoint, CacheTtlSeconds, ClassificationMeasurement,
                Colorizer, ContinuousMeasurement, Coordinate2D, DataId, DataProviderId, DatasetId,
                DateTimeParseFormat, ExternalDataId, FeatureDataType, GdalConfigOption,
                GeoTransform, GridBoundingBox2D, GridIdx2D, LayerId, LinearGradient,
                LogarithmicGradient, Measurement, MlModelName, MlTensorShape3D, MultiLineString,
                MultiPoint, MultiPolygon, NoGeometry, Palette, PlotOutputFormat, RasterColorizer,
                RasterDataType, RasterPropertiesEntryType, RasterPropertiesKey,
                RasterToDatasetQueryRectangle, RgbaColor, SpatialGridDefinition,
                SpatialPartition2D, StringPair, TimeGranularity, TimeInstance, TimeInterval,
                TimeStep, VectorDataType,
            },
            operators::{
                CsvHeader, FileNotFoundHandling, FormatSpecifics, GdalDatasetParameters,
                GdalLoadingInfoTemporalSlice, GdalMetaDataList, GdalMetaDataRegular,
                GdalMetaDataStatic, GdalMetadataMapping, GdalMetadataNetCdfCf,
                GdalSourceTimePlaceholder, LegacyTypedOperator, MlModelMetadata,
                MockDatasetDataSourceLoadingInfo, MockMetaData, OgrMetaData, OgrSourceColumnSpec,
                OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceDurationSpec,
                OgrSourceErrorSpec, OgrSourceTimeFormat, PlotResultDescriptor,
                RasterBandDescriptor, RasterBandDescriptors, RasterResultDescriptor,
                SpatialGridDescriptor, SpatialGridDescriptorState, TimeReference, TypedGeometry,
                TypedResultDescriptor, UnixTimeStampType, VectorColumnInfo, VectorResultDescriptor,
            },
            responses::{
                BadRequestQueryResponse, ErrorResponse, IdResponse, PayloadTooLargeResponse,
                PngResponse, UnauthorizedAdminResponse, UnauthorizedUserResponse,
                UnsupportedMediaTypeForJsonResponse, ZipResponse, datasets::DatasetNameResponse,
                ml_models::MlModelNameResponse,
            },
            services::{
                AddDataset, ArunaDataProviderDefinition, CopernicusDataspaceDataProviderDefinition,
                CreateDataset, DataPath, DatabaseConnectionConfig, Dataset, DatasetDefinition,
                DatasetLayerListingCollection, DatasetLayerListingProviderDefinition,
                EbvPortalDataProviderDefinition, EdrDataProviderDefinition, EdrVectorSpec,
                GbifDataProviderDefinition, GfbioAbcdDataProviderDefinition,
                GfbioCollectionsDataProviderDefinition, LayerProviderListing, MetaDataDefinition,
                MetaDataSuggestion, MlModel, NetCdfCfDataProviderDefinition,
                PangaeaDataProviderDefinition, Provenance, Provenances,
                SentinelS2L2ACogsProviderDefinition, StacApiRetries, StacQueryBuffer,
                TypedDataProviderDefinition, UpdateDataset, Volume,
            },
        },
        ogc::{util::OgcBoundingBox, wcs, wfs, wms},
    },
    contexts::SessionId,
    datasets::{
        DatasetName, RasterDatasetFromWorkflow,
        listing::{DatasetListing, OrderBy},
        storage::{AutoCreateDataset, SuggestMetaData},
        upload::{UploadId, VolumeName},
    },
    layers::{
        layer::{
            AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollection,
            LayerCollectionListing, LayerListing, Property, ProviderLayerCollectionId,
            ProviderLayerId, UpdateLayer, UpdateLayerCollection,
        },
        listing::{
            LayerCollectionId, ProviderCapabilities, SearchCapabilities, SearchType, SearchTypes,
        },
    },
    permissions::{Permission, Role, RoleDescription, RoleId},
    projects::{
        ColorParam, CreateProject, DerivedColor, DerivedNumber, LayerUpdate, LayerVisibility,
        LineSymbology, NumberParam, Plot, PlotUpdate, PointSymbology, PolygonSymbology, Project,
        ProjectId, ProjectLayer, ProjectListing, ProjectUpdateToken, ProjectVersion,
        ProjectVersionId, RasterSymbology, STRectangle, StrokeParam, Symbology, TextSymbology,
        UpdateProject,
    },
    quota::{ComputationId, ComputationQuota, DataUsage, DataUsageSummary, OperatorQuota},
    tasks::{TaskFilter, TaskId, TaskStatus, TaskStatusWithId},
    users::{
        AuthCodeRequestURL, AuthCodeResponse, UserCredentials, UserId, UserInfo, UserRegistration,
        UserSession,
    },
    util::{
        apidoc::{DeriveDiscriminatorMapping, OpenApiServerInfo},
        server::ServerInfo,
    },
    workflows::workflow::{Workflow, WorkflowId},
};
use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::util::server::available_handler,
        crate::util::server::server_info_handler,
        handlers::datasets::auto_create_dataset_handler,
        handlers::datasets::create_dataset_handler,
        handlers::datasets::delete_dataset_handler,
        handlers::datasets::get_dataset_handler,
        handlers::datasets::get_loading_info_handler,
        handlers::datasets::list_datasets_handler,
        handlers::datasets::list_volume_file_layers_handler,
        handlers::datasets::list_volumes_handler,
        handlers::datasets::suggest_meta_data_handler,
        handlers::datasets::update_dataset_handler,
        handlers::datasets::update_dataset_provenance_handler,
        handlers::datasets::update_dataset_symbology_handler,
        handlers::datasets::update_loading_info_handler,
        handlers::datasets::add_dataset_tiles_handler,
        handlers::layers::add_collection,
        handlers::layers::add_existing_collection_to_collection,
        handlers::layers::add_existing_layer_to_collection,
        handlers::layers::add_layer,
        handlers::layers::autocomplete_handler,
        handlers::layers::layer_handler,
        handlers::layers::layer_to_dataset,
        handlers::layers::layer_to_workflow_id_handler,
        handlers::layers::list_collection_handler,
        handlers::layers::list_root_collections_handler,
        handlers::layers::provider_capabilities_handler,
        handlers::layers::remove_collection_from_collection,
        handlers::layers::remove_collection,
        handlers::layers::remove_layer_from_collection,
        handlers::layers::add_provider,
        handlers::layers::get_provider_definition,
        handlers::layers::update_provider_definition,
        handlers::layers::delete_provider,
        handlers::layers::list_providers,
        handlers::users::session_project_handler,
        handlers::users::session_view_handler,
        handlers::layers::remove_layer,
        handlers::layers::search_handler,
        handlers::layers::update_collection,
        handlers::layers::update_layer,
        handlers::machine_learning::add_ml_model,
        handlers::machine_learning::get_ml_model,
        handlers::machine_learning::list_ml_models,
        handlers::permissions::add_permission_handler,
        handlers::permissions::get_resource_permissions_handler,
        handlers::permissions::remove_permission_handler,
        handlers::plots::get_plot_handler,
        handlers::projects::create_project_handler,
        handlers::projects::delete_project_handler,
        handlers::projects::list_projects_handler,
        handlers::projects::load_project_latest_handler,
        handlers::projects::load_project_version_handler,
        handlers::projects::project_versions_handler,
        handlers::projects::update_project_handler,
        handlers::spatial_references::get_spatial_reference_specification_handler,
        handlers::tasks::abort_handler,
        handlers::tasks::list_handler,
        handlers::tasks::status_handler,
        handlers::upload::list_upload_file_layers_handler,
        handlers::upload::list_upload_files_handler,
        handlers::upload::upload_handler,
        handlers::users::add_role_handler,
        handlers::users::anonymous_handler,
        handlers::users::assign_role_handler,
        handlers::users::computation_quota_handler,
        handlers::users::computations_quota_handler,
        handlers::users::data_usage_handler,
        handlers::users::data_usage_summary_handler,
        handlers::users::get_role_by_name_handler,
        handlers::users::get_role_descriptions,
        handlers::users::get_user_quota_handler,
        handlers::users::login_handler,
        handlers::users::logout_handler,
        handlers::users::oidc_init,
        handlers::users::oidc_login,
        handlers::users::quota_handler,
        handlers::users::register_user_handler,
        handlers::users::remove_role_handler,
        handlers::users::revoke_role_handler,
        handlers::users::session_handler,
        handlers::users::update_user_quota_handler,
        handlers::wcs::wcs_handler,
        handlers::wfs::wfs_handler,
        handlers::wms::wms_handler,
        handlers::workflows::dataset_from_workflow_handler,
        handlers::workflows::get_workflow_all_metadata_zip_handler,
        handlers::workflows::get_workflow_metadata_handler,
        handlers::workflows::get_workflow_provenance_handler,
        handlers::workflows::load_workflow_handler,
        handlers::workflows::raster_stream_websocket,
        handlers::workflows::register_workflow_handler,
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
            IdResponse::<RoleId>,
            UnauthorizedAdminResponse,
            UnauthorizedUserResponse,
            BadRequestQueryResponse,
            PngResponse,
            ZipResponse,
        ),
        schemas(
            ErrorResponse,
            UserSession,
            UserCredentials,
            UserRegistration,
            UserInfo,
            Quota,
            UpdateQuota,
            ComputationQuota,
            OperatorQuota,
            DataUsage,
            DataUsageSummary,
            UsageSummaryGranularity,
            AuthCodeResponse,
            AuthCodeRequestURL,

            ComputationId,
            DataId,
            DataProviderId,
            DatasetId,
            DatasetName,
            DatasetNameResponse,
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
            SpatialReferenceSpecification,
            AxisOrder,
            Measurement,
            ContinuousMeasurement,
            ClassificationMeasurement,
            STRectangle,

            ProvenanceEntry,
            Provenance,
            Provenances,

            VectorDataType,
            FeatureDataType,
            RasterDataType,

            ServerInfo,

            Workflow,
            LegacyTypedOperator,
            TypedResultDescriptor,
            PlotResultDescriptor,
            RasterResultDescriptor,
            RasterBandDescriptor,
            RasterBandDescriptors,
            VectorResultDescriptor,
            VectorColumnInfo,
            RasterDatasetFromWorkflow,
            RasterToDatasetQueryRectangle,

            TaskFilter,
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
            UpdateLayer,
            UpdateLayerCollection,
            SearchCapabilities,
            ProviderCapabilities,
            SearchTypes,
            SearchType,

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
            RasterColorizer,
            RgbaColor,
            StrokeParam,
            Symbology,
            TextSymbology,
            LinearGradient,
            LogarithmicGradient,

            OgcBoundingBox,

            wcs::request::WcsRequest,
            wcs::request::WcsService,
            wcs::request::WcsVersion,
            wcs::request::GetCoverageFormat,

            wms::request::WmsRequest,
            wms::request::WmsService,
            wms::request::WmsVersion,
            wms::request::GetMapExceptionFormat,
            wms::request::WmsResponseFormat,

            wfs::request::WfsRequest,
            wfs::request::WfsService,
            wfs::request::WfsVersion,
            wfs::request::TypeNames,

            GeoJson,
            CollectionType,

            UploadFilesResponse,
            UploadFileLayersResponse,
            VolumeFileLayersResponse,

            CreateDataset,
            UpdateDataset,
            AutoCreateDataset,
            SuggestMetaData,
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
            ProjectUpdateToken,
            Plot,
            ProjectLayer,
            LayerVisibility,
            RasterStreamWebsocketResultType,
            CacheTtlSeconds,

            SpatialGridDefinition,
            SpatialGridDescriptorState,
            SpatialGridDescriptor,
            GridBoundingBox2D,
            GridIdx2D,
            GeoTransform,
            TypedDataProviderDefinition,
            ArunaDataProviderDefinition,
            DatasetLayerListingProviderDefinition,
            GbifDataProviderDefinition,
            GfbioAbcdDataProviderDefinition,
            GfbioCollectionsDataProviderDefinition,
            EbvPortalDataProviderDefinition,
            NetCdfCfDataProviderDefinition,
            PangaeaDataProviderDefinition,
            EdrDataProviderDefinition,
            CopernicusDataspaceDataProviderDefinition,
            SentinelS2L2ACogsProviderDefinition,
            DatabaseConnectionConfig,
            EdrVectorSpec,
            StacApiRetries,
            StacQueryBuffer,
            DatasetLayerListingCollection,
            LayerProviderListing,

            PermissionRequest,
            Resource,
            Permission,
            PermissionListing,
            AddRole,
            RoleDescription,
            Role,

            MlModel,
            MlModelName,
            MlModelMetadata,
            MlModelNameResponse,
            MlTensorShape3D,
        ),
    ),
    nest(
        (path = "/processingGraphs", api = crate::api::model::processing_graphs::OperatorsApi),
        (path = "/ogc", api = crate::api::handlers::ogc::OgcApiDoc),
    ),
    modifiers(&SecurityAddon, &ApiDocInfo, &OpenApiServerInfo, &DeriveDiscriminatorMapping),
    external_docs(url = "https://docs.geoengine.io", description = "Geo Engine Docs")
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let Some(components) = openapi.components.as_mut() else {
            debug_assert!(openapi.components.as_mut().is_some());
            return;
        };
        components.add_security_scheme(
            "session_token",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("UUID")
                    .description(Some("A valid session token can be obtained via the /anonymous or /login endpoints."))
                    .build(),
            ),
        );
    }
}

struct ApiDocInfo;

impl Modify for ApiDocInfo {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi.info.title = "Geo Engine API".to_string();

        openapi.info.contact = Some(
            utoipa::openapi::ContactBuilder::new()
                .name(Some("Geo Engine Developers"))
                .email(Some("dev@geoengine.de"))
                .build(),
        );

        openapi.info.license = Some(
            utoipa::openapi::LicenseBuilder::new()
                .name("Apache-2.0")
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
    use crate::{
        contexts::PostgresContext,
        ge_context,
        util::{openapi_examples::can_run_examples, tests::send_test_request},
    };
    use tokio_postgres::NoTls;

    #[test]
    fn can_resolve_api() {
        crate::util::openapi_visitors::can_resolve_api(&ApiDoc::openapi());
    }

    #[ge_context::test]
    async fn it_can_run_examples(app_ctx: PostgresContext<NoTls>) {
        Box::pin(can_run_examples(
            app_ctx,
            ApiDoc::openapi(),
            send_test_request,
        ))
        .await;
    }
}
