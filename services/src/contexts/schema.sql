CREATE TABLE geoengine (
    clear_database_on_start boolean NOT NULL DEFAULT false
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

CREATE TYPE "DefaultColors" AS (
    -- default
    default_color "RgbaColor",
    -- over/under
    over_color "RgbaColor",
    under_color "RgbaColor"
);

CREATE TYPE "ColorizerType" AS ENUM (
    'LinearGradient', 'LogarithmicGradient', 'Palette', 'Rgba'
);

CREATE TYPE "Colorizer" AS (
    "type" "ColorizerType",
    -- linear/logarithmic gradient
    breakpoints "Breakpoint" [],
    no_data_color "RgbaColor",
    color_fields "DefaultColors",
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

CREATE TYPE "RasterSymbology" AS (
    opacity double precision,
    colorizer "Colorizer"
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

CREATE TYPE "RasterResultDescriptor" AS (
    data_type "RasterDataType",
    -- SpatialReferenceOption
    spatial_reference "SpatialReference",
    measurement "Measurement",
    "time" "TimeInterval",
    bbox "SpatialPartition2D",
    resolution "SpatialResolution"
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
    allow_alphaband_as_mask boolean
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

CREATE TABLE layer_providers (
    id uuid PRIMARY KEY,
    type_name text NOT NULL,
    name text NOT NULL,
    definition json NOT NULL
);

-- TODO: relationship between uploads and datasets?
