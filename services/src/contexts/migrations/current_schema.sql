CREATE TABLE geoengine (
    clear_database_on_start boolean NOT NULL DEFAULT false,
    database_version text NOT NULL
);

CREATE TYPE "SpatialReferenceAuthority" AS ENUM (
    'Epsg',
    'SrOrg',
    'Iau2000',
    'Esri'
);

CREATE TYPE "SpatialReference" AS (
    authority "SpatialReferenceAuthority",
    code OID
);

CREATE TYPE "Coordinate2D" AS (
    x double precision,
    y double precision
);

CREATE TYPE "BoundingBox2D" AS (
    lower_left_coordinate "Coordinate2D",
    upper_right_coordinate "Coordinate2D"
);

CREATE TYPE "TimeInterval" AS (
    start bigint,
    "end" bigint
);

CREATE TYPE "STRectangle" AS (
    spatial_reference "SpatialReference",
    bounding_box "BoundingBox2D",
    time_interval "TimeInterval"
);

CREATE TYPE "TimeGranularity" AS ENUM (
    'Millis',
    'Seconds',
    'Minutes',
    'Hours',
    'Days',
    'Months',
    'Years'
);

CREATE TYPE "TimeStep" AS (
    granularity "TimeGranularity",
    step OID
);

CREATE TYPE "DatasetName" AS (namespace text, name text);

CREATE TYPE "Provenance" AS (
    citation text,
    license text,
    uri text
);

CREATE DOMAIN "RgbaColor" AS smallint [4] CHECK (
    0 <= ALL(value) AND 255 >= ALL(value)
);

CREATE TYPE "Breakpoint" AS (
    "value" double precision,
    color "RgbaColor"
);

CREATE TYPE "ColorizerType" AS ENUM (
    'LinearGradient', 'LogarithmicGradient', 'Palette'
);

CREATE TYPE "Colorizer" AS (
    "type" "ColorizerType",
    -- linear/logarithmic gradient
    breakpoints "Breakpoint" [],
    no_data_color "RgbaColor",
    over_color "RgbaColor",
    under_color "RgbaColor",
    -- palette
    -- (colors --> breakpoints)
    default_color "RgbaColor"
    -- (no_data_color)
    -- rgba
    -- (nothing)
);

CREATE TYPE "ColorParam" AS (
    -- static
    color "RgbaColor",
    -- derived
    attribute text,
    colorizer "Colorizer"
);

CREATE TYPE "NumberParam" AS (
    -- static
    "value" bigint,
    -- derived
    attribute text,
    factor double precision,
    default_value double precision
);

CREATE TYPE "StrokeParam" AS (
    width "NumberParam",
    color "ColorParam"
);

CREATE TYPE "TextSymbology" AS (
    attribute text,
    fill_color "ColorParam",
    stroke "StrokeParam"
);

CREATE TYPE "PointSymbology" AS (
    radius "NumberParam",
    fill_color "ColorParam",
    stroke "StrokeParam",
    text "TextSymbology"
);

CREATE TYPE "LineSymbology" AS (
    stroke "StrokeParam",
    text "TextSymbology",
    auto_simplified boolean
);

CREATE TYPE "PolygonSymbology" AS (
    fill_color "ColorParam",
    stroke "StrokeParam",
    text "TextSymbology",
    auto_simplified boolean
);

CREATE TYPE "RasterColorizerType" AS ENUM (
    'SingleBand',
    'MultiBand'
);

CREATE TYPE "RasterColorizer" AS (
    "type" "RasterColorizerType",
    -- single band colorizer
    band bigint,
    band_colorizer "Colorizer",
    -- multi band colorizer
    red_band bigint,
    green_band bigint,
    blue_band bigint,
    red_min double precision,
    red_max double precision,
    red_scale double precision,
    green_min double precision,
    green_max double precision,
    green_scale double precision,
    blue_min double precision,
    blue_max double precision,
    blue_scale double precision,
    no_data_color "RgbaColor"
);

CREATE TYPE "RasterSymbology" AS (
    opacity double precision,
    raster_colorizer "RasterColorizer"
);

CREATE TYPE "Symbology" AS (
    "raster" "RasterSymbology",
    "point" "PointSymbology",
    "line" "LineSymbology",
    "polygon" "PolygonSymbology"
);

CREATE TYPE "RasterDataType" AS ENUM (
    'U8',
    'U16',
    'U32',
    'U64',
    'I8',
    'I16',
    'I32',
    'I64',
    'F32',
    'F64'
);

CREATE TYPE "ContinuousMeasurement" AS (
    measurement text,
    unit text
);

CREATE TYPE "SmallintTextKeyValue" AS (
    key smallint,
    value text
);

CREATE TYPE "TextTextKeyValue" AS (
    key text,
    value text
);

CREATE TYPE "ClassificationMeasurement" AS (
    measurement text,
    classes "SmallintTextKeyValue" []
);

CREATE TYPE "Measurement" AS (
    -- "unitless" if all none
    continuous "ContinuousMeasurement",
    classification "ClassificationMeasurement"
);

CREATE TYPE "SpatialPartition2D" AS (
    upper_left_coordinate "Coordinate2D",
    lower_right_coordinate "Coordinate2D"
);

CREATE TYPE "SpatialResolution" AS (
    x double precision,
    y double precision
);

CREATE TYPE "VectorDataType" AS ENUM (
    'Data',
    'MultiPoint',
    'MultiLineString',
    'MultiPolygon'
);

CREATE TYPE "FeatureDataType" AS ENUM (
    'Category',
    'Int',
    'Float',
    'Text',
    'Bool',
    'DateTime'
);

CREATE TYPE "VectorColumnInfo" AS (
    "column" text,
    data_type "FeatureDataType",
    measurement "Measurement"
);

CREATE TYPE "RasterBandDescriptor" AS (
    "name" text,
    measurement "Measurement"
);

CREATE TYPE "RasterResultDescriptor" AS (
    data_type "RasterDataType",
    -- SpatialReferenceOption
    spatial_reference "SpatialReference",
    "time" "TimeInterval",
    bbox "SpatialPartition2D",
    resolution "SpatialResolution",
    bands "RasterBandDescriptor" []
);

CREATE TYPE "VectorResultDescriptor" AS (
    data_type "VectorDataType",
    -- SpatialReferenceOption
    spatial_reference "SpatialReference",
    columns "VectorColumnInfo" [],
    "time" "TimeInterval",
    bbox "BoundingBox2D"
);

CREATE TYPE "PlotResultDescriptor" AS (
    -- SpatialReferenceOption
    spatial_reference "SpatialReference",
    "time" "TimeInterval",
    bbox "BoundingBox2D"
);

CREATE TYPE "ResultDescriptor" AS (
    -- oneOf
    raster "RasterResultDescriptor",
    vector "VectorResultDescriptor",
    plot "PlotResultDescriptor"
);

CREATE TYPE "MockDatasetDataSourceLoadingInfo" AS (
    points "Coordinate2D" []
);

CREATE TYPE "DateTimeParseFormat" AS (
    fmt text,
    has_tz boolean,
    has_time boolean
);

CREATE TYPE "OgrSourceTimeFormatCustom" AS (
    custom_format "DateTimeParseFormat"
);

CREATE TYPE "UnixTimeStampType" AS ENUM (
    'EpochSeconds',
    'EpochMilliseconds'
);

CREATE TYPE "OgrSourceTimeFormatUnixTimeStamp" AS (
    timestamp_type "UnixTimeStampType",
    fmt "DateTimeParseFormat"
);

CREATE TYPE "OgrSourceTimeFormat" AS (
    -- oneOf
    -- Auto
    custom "OgrSourceTimeFormatCustom",
    unix_time_stamp "OgrSourceTimeFormatUnixTimeStamp"
);

CREATE TYPE "OgrSourceDurationSpec" AS (
    -- oneOf
    infinite boolean, -- void
    zero boolean, -- void
    "value" "TimeStep"
);

CREATE TYPE "OgrSourceDatasetTimeTypeStart" AS (
    start_field text,
    start_format "OgrSourceTimeFormat",
    duration "OgrSourceDurationSpec"
);

CREATE TYPE "OgrSourceDatasetTimeTypeStartEnd" AS (
    start_field text,
    start_format "OgrSourceTimeFormat",
    end_field text,
    end_format "OgrSourceTimeFormat"
);

CREATE TYPE "OgrSourceDatasetTimeTypeStartDuration" AS (
    start_field text,
    start_format "OgrSourceTimeFormat",
    duration_field text
);

CREATE TYPE "OgrSourceDatasetTimeType" AS (
    -- oneOf
    -- None
    "start" "OgrSourceDatasetTimeTypeStart",
    start_end "OgrSourceDatasetTimeTypeStartEnd",
    start_duration "OgrSourceDatasetTimeTypeStartDuration"
);

CREATE TYPE "CsvHeader" AS ENUM (
    'Yes',
    'No',
    'Auto'
);

CREATE TYPE "FormatSpecificsCsv" AS (
    header "CsvHeader"
);

CREATE TYPE "FormatSpecifics" AS (
    -- oneOf
    csv "FormatSpecificsCsv"
);

CREATE TYPE "OgrSourceColumnSpec" AS (
    format_specifics "FormatSpecifics",
    x text,
    y text,
    "int" text [],
    "float" text [],
    "text" text [],
    bool text [],
    "datetime" text [],
    rename "TextTextKeyValue" []
);

CREATE TYPE "OgrSourceErrorSpec" AS ENUM (
    'Ignore',
    'Abort'
);

-- We store `Polygon`s as an array of rings that are closed postgres `path`s.
-- We do not use an array of `polygon`s as it is the same as storing a path 
-- plus a stored bbox that we don't want to compute and store (overhead).
CREATE DOMAIN "Polygon" AS path [];

CREATE TYPE "TypedGeometry" AS (
    -- oneOf
    "data" boolean, -- void
    multi_point point [],
    multi_line_string path [],
    multi_polygon "Polygon" []
);

CREATE TYPE "OgrSourceDataset" AS (
    file_name text,
    layer_name text,
    data_type "VectorDataType",
    "time" "OgrSourceDatasetTimeType",
    default_geometry "TypedGeometry",
    columns "OgrSourceColumnSpec",
    force_ogr_time_filter boolean,
    force_ogr_spatial_filter boolean,
    on_error "OgrSourceErrorSpec",
    sql_query text,
    attribute_query text,
    cache_ttl int
);

CREATE TYPE "MockMetaData" AS (
    loading_info "MockDatasetDataSourceLoadingInfo",
    result_descriptor "VectorResultDescriptor"
);

CREATE TYPE "OgrMetaData" AS (
    loading_info "OgrSourceDataset",
    result_descriptor "VectorResultDescriptor"
);

CREATE TYPE "GdalDatasetGeoTransform" AS (
    origin_coordinate "Coordinate2D",
    x_pixel_size double precision,
    y_pixel_size double precision
);

CREATE TYPE "FileNotFoundHandling" AS ENUM (
    'NoData',
    'Error'
);

CREATE TYPE "RasterPropertiesKey" AS (
    domain text,
    key text
);

CREATE TYPE "RasterPropertiesEntryType" AS ENUM (
    'Number',
    'String'
);

CREATE TYPE "GdalMetadataMapping" AS (
    source_key "RasterPropertiesKey",
    target_key "RasterPropertiesKey",
    target_type "RasterPropertiesEntryType"
);

CREATE DOMAIN "StringPair" AS text [2];

CREATE TYPE "GdalRetryOptions" AS (
    max_retries bigint
);

CREATE TYPE "GdalDatasetParameters" AS (
    file_path text,
    rasterband_channel bigint,
    geo_transform "GdalDatasetGeoTransform",
    width bigint,
    height bigint,
    file_not_found_handling "FileNotFoundHandling",
    no_data_value double precision,
    properties_mapping "GdalMetadataMapping" [],
    gdal_open_options text [],
    gdal_config_options "StringPair" [],
    allow_alphaband_as_mask boolean,
    retry "GdalRetryOptions"
);

CREATE TYPE "TimeReference" AS ENUM (
    'Start',
    'End'
);

CREATE TYPE "GdalSourceTimePlaceholder" AS (
    "format" "DateTimeParseFormat",
    reference "TimeReference"
);

CREATE TYPE "TextGdalSourceTimePlaceholderKeyValue" AS (
    "key" text,
    "value" "GdalSourceTimePlaceholder"
);

CREATE TYPE "GdalMetaDataRegular" AS (
    result_descriptor "RasterResultDescriptor",
    params "GdalDatasetParameters",
    time_placeholders "TextGdalSourceTimePlaceholderKeyValue" [],
    data_time "TimeInterval",
    step "TimeStep",
    cache_ttl int
);

CREATE TYPE "GdalMetaDataStatic" AS (
    time "TimeInterval",
    params "GdalDatasetParameters",
    result_descriptor "RasterResultDescriptor",
    cache_ttl int
);

CREATE TYPE "GdalMetadataNetCdfCf" AS (
    result_descriptor "RasterResultDescriptor",
    params "GdalDatasetParameters",
    "start" bigint,
    "end" bigint,
    step "TimeStep",
    band_offset bigint,
    cache_ttl int
);

CREATE TYPE "GdalLoadingInfoTemporalSlice" AS (
    time "TimeInterval",
    params "GdalDatasetParameters",
    cache_ttl int
);

CREATE TYPE "GdalMetaDataList" AS (
    result_descriptor "RasterResultDescriptor",
    params "GdalLoadingInfoTemporalSlice" []
);

CREATE TYPE "MetaDataDefinition" AS (
    -- oneOf
    mock_meta_data "MockMetaData",
    ogr_meta_data "OgrMetaData",
    gdal_meta_data_regular "GdalMetaDataRegular",
    gdal_static "GdalMetaDataStatic",
    gdal_metadata_net_cdf_cf "GdalMetadataNetCdfCf",
    gdal_meta_data_list "GdalMetaDataList"
);

-- seperate table for projects used in foreign key constraints
CREATE TABLE projects (id uuid PRIMARY KEY);

CREATE TABLE sessions (
    id uuid PRIMARY KEY,
    project_id uuid REFERENCES projects (id) ON DELETE
    SET
    NULL,
    view "STRectangle"
);

CREATE TABLE project_versions (
    id uuid PRIMARY KEY,
    project_id uuid REFERENCES projects (id) ON DELETE CASCADE NOT NULL,
    name character varying(256) NOT NULL,
    description text NOT NULL,
    bounds "STRectangle" NOT NULL,
    time_step "TimeStep" NOT NULL,
    changed timestamp
    with time zone NOT NULL
);

CREATE INDEX project_version_idx ON project_versions (project_id, changed DESC);

CREATE TYPE "LayerType" AS ENUM ('Raster', 'Vector');

CREATE TYPE "LayerVisibility" AS (data BOOLEAN, legend BOOLEAN);

CREATE TABLE project_version_layers (
    layer_index integer NOT NULL,
    project_id uuid REFERENCES projects (id) ON DELETE CASCADE NOT NULL,
    project_version_id uuid REFERENCES project_versions (
        id
    ) ON DELETE CASCADE NOT NULL,
    name character varying(256) NOT NULL,
    workflow_id uuid NOT NULL,
    -- TODO: REFERENCES workflows(id)
    symbology "Symbology",
    visibility "LayerVisibility" NOT NULL,
    PRIMARY KEY (
        project_id,
        project_version_id,
        layer_index
    )
);

CREATE TABLE project_version_plots (
    plot_index integer NOT NULL,
    project_id uuid REFERENCES projects (id) ON DELETE CASCADE NOT NULL,
    project_version_id uuid REFERENCES project_versions (
        id
    ) ON DELETE CASCADE NOT NULL,
    name character varying(256) NOT NULL,
    workflow_id uuid NOT NULL,
    -- TODO: REFERENCES workflows(id)
    PRIMARY KEY (
        project_id,
        project_version_id,
        plot_index
    )
);

CREATE TABLE workflows (
    id uuid PRIMARY KEY,
    workflow json NOT NULL
);

-- TODO: add constraint not null

-- TODO: add length constraints

CREATE TABLE datasets (
    id uuid PRIMARY KEY,
    name "DatasetName" UNIQUE NOT NULL,
    display_name text NOT NULL,
    description text NOT NULL,
    tags text [],
    source_operator text NOT NULL,
    result_descriptor "ResultDescriptor" NOT NULL,
    meta_data "MetaDataDefinition" NOT NULL,
    symbology "Symbology",
    provenance "Provenance" []
);

-- TODO: add constraint not null

-- TODO: add constaint byte_size >= 0

CREATE TYPE "FileUpload" AS (
    id UUID,
    name text,
    byte_size bigint
);

-- TODO: time of creation and last update

-- TODO: upload directory that is not directly derived from id

CREATE TABLE uploads (
    id uuid PRIMARY KEY,
    -- user_id UUID REFERENCES users(id) ON DELETE CASCADE NOT NULL,
    files "FileUpload" [] NOT NULL
);

CREATE TYPE "PropertyType" AS (key text, value text);

CREATE TABLE layer_collections (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    description text NOT NULL,
    properties "PropertyType" [] NOT NULL
);

CREATE TABLE layers (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    description text NOT NULL,
    workflow_id uuid REFERENCES workflows (id) NOT NULL,
    symbology "Symbology",
    properties "PropertyType" [] NOT NULL,
    metadata "TextTextKeyValue" [] NOT NULL
);

CREATE TABLE collection_layers (
    collection uuid REFERENCES layer_collections (
        id
    ) ON DELETE CASCADE NOT NULL,
    layer uuid REFERENCES layers (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (collection, layer)
);

CREATE TABLE collection_children (
    parent uuid REFERENCES layer_collections (id) ON DELETE CASCADE NOT NULL,
    child uuid REFERENCES layer_collections (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (parent, child)
);

CREATE TYPE "ArunaDataProviderDefinition" AS (
    id uuid,
    "name" text,
    api_url text,
    project_id text,
    api_token text,
    filter_label text,
    cache_ttl int,
    description text,
    priority smallint
);

CREATE TYPE "DatabaseConnectionConfig" AS (
    host text,
    port int,
    "database" text,
    schema text,
    "user" text,
    "password" text
);

CREATE TYPE "GbifDataProviderDefinition" AS (
    "name" text,
    db_config "DatabaseConnectionConfig",
    cache_ttl int,
    autocomplete_timeout int,
    description text,
    priority smallint,
    columns text []
);

CREATE TYPE "GfbioAbcdDataProviderDefinition" AS (
    "name" text,
    db_config "DatabaseConnectionConfig",
    cache_ttl int,
    description text,
    priority smallint
);

CREATE TYPE "GfbioCollectionsDataProviderDefinition" AS (
    "name" text,
    collection_api_url text,
    collection_api_auth_token text,
    abcd_db_config "DatabaseConnectionConfig",
    pangaea_url text,
    cache_ttl int,
    description text,
    priority smallint
);

CREATE TYPE "EbvPortalDataProviderDefinition" AS (
    "name" text,
    "data" text,
    base_url text,
    overviews text,
    cache_ttl int,
    description text,
    priority smallint
);

CREATE TYPE "NetCdfCfDataProviderDefinition" AS (
    "name" text,
    "data" text,
    overviews text,
    cache_ttl int,
    description text,
    priority smallint
);

CREATE TYPE "PangaeaDataProviderDefinition" AS (
    "name" text,
    base_url text,
    cache_ttl int,
    description text,
    priority smallint
);

CREATE TYPE "EdrVectorSpec" AS (
    x text,
    y text,
    "time" text
);

CREATE TYPE "EdrDataProviderDefinition" AS (
    "name" text,
    id uuid,
    base_url text,
    vector_spec "EdrVectorSpec",
    cache_ttl int,
    discrete_vrs text [],
    provenance "Provenance" [],
    description text,
    priority smallint
);

CREATE TYPE "DatasetLayerListingCollection" AS (
    "name" text,
    description text,
    tags text []
);

CREATE TYPE "DatasetLayerListingProviderDefinition" AS (
    id uuid,
    "name" text,
    description text,
    collections "DatasetLayerListingCollection" [],
    priority smallint
);

CREATE TYPE "DataProviderDefinition" AS (
    -- one of
    aruna_data_provider_definition "ArunaDataProviderDefinition",
    gbif_data_provider_definition "GbifDataProviderDefinition",
    gfbio_abcd_data_provider_definition "GfbioAbcdDataProviderDefinition",
    gfbio_collections_data_provider_definition
    "GfbioCollectionsDataProviderDefinition",
    ebv_portal_data_provider_definition "EbvPortalDataProviderDefinition",
    net_cdf_cf_data_provider_definition "NetCdfCfDataProviderDefinition",
    pangaea_data_provider_definition "PangaeaDataProviderDefinition",
    edr_data_provider_definition "EdrDataProviderDefinition",
    dataset_layer_listing_provider_definition
    "DatasetLayerListingProviderDefinition"
);

CREATE TABLE layer_providers (
    id uuid PRIMARY KEY,
    type_name text NOT NULL,
    name text NOT NULL,
    definition "DataProviderDefinition" NOT NULL,
    priority smallint NOT NULL DEFAULT 0
);

-- TODO: relationship between uploads and datasets?

-- EBV PROVIDER TABLE DEFINITIONS

CREATE TABLE ebv_provider_dataset_locks (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name)
);

CREATE TABLE ebv_provider_overviews (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    title text NOT NULL,
    summary text NOT NULL,
    spatial_reference "SpatialReference" NOT NULL,
    colorizer "Colorizer" NOT NULL,
    creator_name text,
    creator_email text,
    creator_institution text,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name)
);

CREATE TABLE ebv_provider_groups (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    name text [] NOT NULL,
    title text NOT NULL,
    description text NOT NULL,
    data_type "RasterDataType",
    data_range float [2],
    unit text NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name, name) DEFERRABLE,

    FOREIGN KEY (provider_id, file_name) REFERENCES ebv_provider_overviews (
        provider_id,
        file_name
    ) ON DELETE CASCADE DEFERRABLE
);

CREATE TABLE ebv_provider_entities (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    id bigint NOT NULL,
    name text NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name, id) DEFERRABLE,

    FOREIGN KEY (provider_id, file_name) REFERENCES ebv_provider_overviews (
        provider_id,
        file_name
    ) ON DELETE CASCADE DEFERRABLE
);

CREATE TABLE ebv_provider_timestamps (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    "time" bigint NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name, "time") DEFERRABLE,

    FOREIGN KEY (provider_id, file_name) REFERENCES ebv_provider_overviews (
        provider_id,
        file_name
    ) ON DELETE CASCADE DEFERRABLE
);

CREATE TABLE ebv_provider_loading_infos (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    group_names text [] NOT NULL,
    entity_id bigint NOT NULL,
    meta_data "GdalMetaDataList" NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name, group_names, entity_id) DEFERRABLE,

    FOREIGN KEY (provider_id, file_name) REFERENCES ebv_provider_overviews (
        provider_id,
        file_name
    ) ON DELETE CASCADE DEFERRABLE
);

CREATE TYPE "MlModelMetadata" AS (
    file_name text,
    input_type "RasterDataType",
    num_input_bands OID,
    output_type "RasterDataType"
);

CREATE TYPE "MlModelName" AS (namespace text, name text);

CREATE TABLE ml_models ( -- noqa: 
    id uuid PRIMARY KEY,
    "name" "MlModelName" UNIQUE NOT NULL,
    display_name text NOT NULL,
    description text NOT NULL,
    upload uuid REFERENCES uploads (id) ON DELETE CASCADE NOT NULL,
    metadata "MlModelMetadata"
);
