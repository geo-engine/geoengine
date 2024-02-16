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
