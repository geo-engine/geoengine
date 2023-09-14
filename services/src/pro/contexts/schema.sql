CREATE TYPE "StacBand" AS (
    "name" text,
    no_data_value double precision,
    data_type "RasterDataType"
);

CREATE TYPE "StacZone" AS (
    "name" text,
    epsg oid
);

CREATE TYPE "StacApiRetries" AS (
    number_of_retries bigint,
    initial_delay_ms bigint,
    exponential_backoff_factor double precision
);

CREATE TYPE "GdalRetries" AS (
    number_of_retries bigint
);

CREATE TYPE "SentinelS2L2ACogsProviderDefinition" AS (
    "name" text,
    id uuid,
    api_url text,
    bands "StacBand" [],
    zones "StacZone" [],
    stac_api_retries "StacApiRetries",
    gdal_retries "GdalRetries",
    cache_ttl int
);

CREATE TYPE "ProDataProviderDefinition" AS (
    -- one of
    sentinel_s2_l2_a_cogs_provider_definition
    "SentinelS2L2ACogsProviderDefinition"
);

CREATE TABLE pro_layer_providers (
    id uuid PRIMARY KEY,
    type_name text NOT NULL,
    name text NOT NULL,
    definition "ProDataProviderDefinition" NOT NULL
);

-- TODO: distinguish between roles that are (correspond to) users 
--       and roles that are not

-- TODO: integrity constraint for roles that correspond to users
--       + DELETE CASCADE

CREATE TABLE roles (
    id uuid PRIMARY KEY,
    name text UNIQUE NOT NULL
);

CREATE TABLE users (
    id uuid PRIMARY KEY REFERENCES roles (id),
    email character varying(256) UNIQUE,
    password_hash character varying(256),
    real_name character varying(256),
    active boolean NOT NULL,
    quota_available bigint NOT NULL DEFAULT 0,
    quota_used bigint NOT NULL DEFAULT 0,
    -- TODO: rename to total_quota_used?
    CONSTRAINT users_anonymous_ck CHECK ((
        email IS NULL
        AND password_hash IS NULL
        AND real_name IS NULL
    )
    OR (
        email IS NOT NULL
        AND password_hash IS NOT NULL
        AND real_name IS NOT NULL
    )
    ),
    CONSTRAINT users_quota_used_ck CHECK (quota_used >= 0)
);

-- relation between users and roles

-- all users have a default role where role_id = user_id

CREATE TABLE user_roles (
    user_id uuid REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    role_id uuid REFERENCES roles (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE user_sessions (
    user_id uuid REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    session_id uuid REFERENCES sessions (id) ON DELETE CASCADE NOT NULL,
    created timestamp with time zone NOT NULL,
    valid_until timestamp with time zone NOT NULL,
    PRIMARY KEY (user_id, session_id)
);

CREATE TABLE project_version_authors (
    project_version_id uuid REFERENCES project_versions (
        id
    ) ON DELETE CASCADE NOT NULL,
    user_id uuid REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (project_version_id, user_id)
);

CREATE TABLE user_uploads (
    user_id uuid REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    upload_id uuid REFERENCES uploads (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (user_id, upload_id)
);

CREATE TYPE "Permission" AS ENUM ('Read', 'Owner');

-- TODO: uploads, providers permissions

-- TODO: relationship between uploads and datasets?

CREATE TABLE external_users (
    id uuid PRIMARY KEY REFERENCES users (id),
    external_id character varying(256) UNIQUE,
    email character varying(256),
    real_name character varying(256),
    active boolean NOT NULL
);

CREATE TABLE permissions (
    -- resource_type "ResourceType" NOT NULL,
    role_id uuid REFERENCES roles (id) ON DELETE CASCADE NOT NULL,
    permission "Permission" NOT NULL,
    dataset_id uuid REFERENCES datasets (id) ON DELETE CASCADE,
    layer_id uuid REFERENCES layers (id) ON DELETE CASCADE,
    layer_collection_id uuid REFERENCES layer_collections (
        id
    ) ON DELETE CASCADE,
    project_id uuid REFERENCES projects (id) ON DELETE CASCADE,
    CHECK (
        (
            (dataset_id IS NOT NULL)::integer
            + (layer_id IS NOT NULL)::integer
            + (layer_collection_id IS NOT NULL)::integer
            + (project_id IS NOT NULL)::integer
        ) = 1
    )
);

CREATE UNIQUE INDEX ON permissions (
    role_id,
    permission,
    dataset_id
);

CREATE UNIQUE INDEX ON permissions (role_id, permission, layer_id);

CREATE UNIQUE INDEX ON permissions (
    role_id,
    permission,
    layer_collection_id
);

CREATE UNIQUE INDEX ON permissions (
    role_id,
    permission,
    project_id
);

CREATE VIEW user_permitted_datasets
AS
SELECT
    r.user_id,
    p.dataset_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.dataset_id IS NOT NULL
);

CREATE VIEW user_permitted_projects
AS
SELECT
    r.user_id,
    p.project_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.project_id IS NOT NULL
);

CREATE VIEW user_permitted_layer_collections
AS
SELECT
    r.user_id,
    p.layer_collection_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.layer_collection_id IS NOT NULL
);

CREATE VIEW user_permitted_layers
AS
SELECT
    r.user_id,
    p.layer_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.layer_id IS NOT NULL
);
