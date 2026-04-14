"""Entry point for Geo Engine Python Library"""

import geoengine_openapi_client
from geoengine_openapi_client import UsageSummaryGranularity
from geoengine_openapi_client.exceptions import (
    ApiAttributeError,
    ApiException,
    ApiKeyError,
    ApiTypeError,
    ApiValueError,
    BadRequestException,
    NotFoundException,
    OpenApiException,
)
from pydantic import ValidationError
from requests import utils

from . import workflow_builder
from .auth import Session, get_session, initialize, reset
from .colorizer import (
    ColorBreakpoint,
    Colorizer,
    LinearGradientColorizer,
    LogarithmicGradientColorizer,
    PaletteColorizer,
)
from .datasets import (
    AddDatasetProperties,
    DatasetListOrder,
    OgrOnError,
    OgrSourceDatasetTimeType,
    StoredDataset,
    add_dataset,
    add_or_replace_dataset_with_permissions,
    dataset_info_by_name,
    dataset_metadata_by_name,
    delete_dataset,
    list_datasets,
    upload_dataframe,
    volume_by_name,
    volumes,
)
from .error import (
    GeoEngineException,
    InputException,
    InvalidUrlException,
    MethodNotCalledOnPlotException,
    MethodNotCalledOnRasterException,
    MethodNotCalledOnVectorException,
    MissingFieldInResponseException,
    ModificationNotOnLayerDbException,
    OGCXMLError,
    SpatialReferenceMismatchException,
    TypeException,
    UninitializedException,
    check_response_for_error,
)
from .layers import Layer, LayerCollection, LayerCollectionListing, LayerListing, layer, layer_collection
from .ml import MlModelConfig, register_ml_model
from .permissions import (
    ADMIN_ROLE_ID,
    ANONYMOUS_USER_ROLE_ID,
    REGISTERED_USER_ROLE_ID,
    Permission,
    RoleId,
    UserId,
    add_permission,
    add_role,
    assign_role,
    remove_permission,
    remove_role,
    revoke_role,
)
from .raster import RasterTile2D
from .raster_workflow_rio_writer import RasterWorkflowRioWriter
from .resource_identifier import (
    LAYER_DB_PROVIDER_ID,
    LAYER_DB_ROOT_COLLECTION_ID,
    DatasetName,
    LayerCollectionId,
    LayerId,
    LayerProviderId,
    MlModelName,
    Resource,
    UploadId,
)
from .tasks import Task, TaskId
from .types import (
    DEFAULT_ISO_TIME_FORMAT,
    BoundingBox2D,
    ClassificationMeasurement,
    ContinuousMeasurement,
    FeatureDataType,
    GeoTransform,
    GridBoundingBox2D,
    GridIdx2D,
    IrregularTimeDimension,
    Measurement,
    MultiBandRasterColorizer,
    Provenance,
    QueryRectangle,
    RasterBandDescriptor,
    RasterColorizer,
    RasterDataType,
    RasterQueryRectangle,
    RasterResultDescriptor,
    RasterSymbology,
    RegularTimeDimension,
    SingleBandRasterColorizer,
    SpatialGridDefinition,
    SpatialGridDescriptor,
    SpatialPartition2D,
    SpatialResolution,
    TimeDescriptor,
    TimeDimension,
    TimeInterval,
    TimeStep,
    TimeStepGranularity,
    UnitlessMeasurement,
    VectorColumnInfo,
    VectorDataType,
    VectorResultDescriptor,
    VectorSymbology,
)
from .util import clamp_datetime_ms_ns
from .workflow import (
    Workflow,
    WorkflowId,
    data_usage,
    data_usage_summary,
    get_quota,
    register_workflow,
    update_quota,
    workflow_by_id,
)

DEFAULT_USER_AGENT = f"geoengine-python/{geoengine_openapi_client.__version__}"


def default_user_agent(_name="python-requests"):
    return DEFAULT_USER_AGENT


utils.default_user_agent = default_user_agent
