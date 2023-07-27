CREATE TABLE geoengine (
    production boolean NOT NULL DEFAULT false
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
    start timestamp with time zone,
    "end" timestamp with time zone
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
    symbology json,
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

CREATE TYPE "DatasetName" AS (namespace text, name text);

CREATE TABLE datasets (
    id uuid PRIMARY KEY,
    name "DatasetName" UNIQUE NOT NULL,
    display_name text NOT NULL,
    description text NOT NULL,
    tags text [],
    source_operator text NOT NULL,
    result_descriptor json NOT NULL,
    meta_data json NOT NULL,
    symbology json,
    provenance json
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
    workflow json NOT NULL,
    symbology json,
    properties "PropertyType" [] NOT NULL,
    metadata json NOT NULL
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
