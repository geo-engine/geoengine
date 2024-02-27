-- VERSIONING

CREATE TABLE geoengine (
    database_version text NOT NULL
);

INSERT INTO geoengine (database_version) VALUES (
    '0000_initial'
);


-- TYPE DEFINITIONS

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

CREATE DOMAIN "RgbaColor" AS smallint [4] CHECK (
    0 <= ALL(value) AND 255 >= ALL(value)
);

CREATE TYPE "Breakpoint" AS (
    "value" double precision,
    color "RgbaColor"
);

CREATE TYPE "ColorizerType" AS ENUM (
    'LinearGradient', 'LogarithmicGradient', 'Palette', 'Rgba'
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

CREATE TYPE "TimeInterval" AS (
    start bigint,
    "end" bigint
);

CREATE TYPE "SpatialResolution" AS (
    x double precision,
    y double precision
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

CREATE TYPE "RasterBandDescriptor" AS (
    "name" text,
    measurement "Measurement"
);

CREATE TYPE "RasterResultDescriptor" AS (
    data_type "RasterDataType",
    -- SpatialReferenceOption
    spatial_reference "SpatialReference",
    bands "RasterBandDescriptor" [],
    "time" "TimeInterval",
    bbox "SpatialPartition2D",
    resolution "SpatialResolution"
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

CREATE TYPE "GdalLoadingInfoTemporalSlice" AS (
    time "TimeInterval",
    params "GdalDatasetParameters",
    cache_ttl int
);

CREATE TYPE "GdalMetaDataList" AS (
    result_descriptor "RasterResultDescriptor",
    params "GdalLoadingInfoTemporalSlice" []
);



-- TABLE DEFINITIONS

CREATE TABLE overviews (
    file_name text PRIMARY KEY NOT NULL,
    title text NOT NULL,
    summary text NOT NULL,
    spatial_reference "SpatialReference" NOT NULL,
    colorizer "Colorizer" NOT NULL,
    creator_name text,
    creator_email text,
    creator_institution text
);

CREATE TABLE groups (
    file_name text NOT NULL REFERENCES overviews (
        file_name
    ) ON DELETE CASCADE DEFERRABLE,
    name text [] NOT NULL,
    title text NOT NULL,
    description text NOT NULL,
    data_type "RasterDataType",
    data_range float [2],
    unit text NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (file_name, name) DEFERRABLE
);

CREATE TABLE entities (
    file_name text NOT NULL REFERENCES overviews (
        file_name
    ) ON DELETE CASCADE DEFERRABLE,
    id bigint NOT NULL,
    name text NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (file_name, id) DEFERRABLE
);

CREATE TABLE timestamps (
    file_name text NOT NULL REFERENCES overviews (
        file_name
    ) ON DELETE CASCADE DEFERRABLE,
    "time" bigint NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (file_name, "time") DEFERRABLE
);

CREATE TABLE loading_infos (
    file_name text NOT NULL REFERENCES overviews (
        file_name
    ) ON DELETE CASCADE DEFERRABLE,
    group_names text [] NOT NULL,
    entity_id bigint NOT NULL,
    meta_data "GdalMetaDataList" NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (file_name, group_names, entity_id) DEFERRABLE
);
