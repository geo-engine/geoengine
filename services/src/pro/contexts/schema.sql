-- TODO: distinguish between roles that are (correspond to) users 
--       and roles that are not

-- TODO: integrity constraint for roles that correspond to users
--       + DELETE CASCADE

CREATE TABLE roles (
    id UUID PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE users (
    id UUID PRIMARY KEY REFERENCES roles (id),
    email CHARACTER VARYING(256) UNIQUE,
    password_hash CHARACTER VARYING(256),
    real_name CHARACTER VARYING(256),
    active BOOLEAN NOT NULL,
    quota_available BIGINT NOT NULL DEFAULT 0,
    quota_used BIGINT NOT NULL DEFAULT 0,
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
    user_id UUID REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    role_id UUID REFERENCES roles (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE user_sessions (
    user_id UUID REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    session_id UUID REFERENCES sessions (id) ON DELETE CASCADE NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_until TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (user_id, session_id)
);

CREATE TABLE project_version_authors (
    project_version_id UUID REFERENCES project_versions (
        id
    ) ON DELETE CASCADE NOT NULL,
    user_id UUID REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (project_version_id, user_id)
);

CREATE TABLE user_uploads (
    user_id UUID REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    upload_id UUID REFERENCES uploads (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (user_id, upload_id)
);

CREATE TYPE "Permission" AS ENUM ('Read', 'Owner');

-- TODO: uploads, providers permissions

-- TODO: relationship between uploads and datasets?

CREATE TABLE external_users (
    id UUID PRIMARY KEY REFERENCES users (id),
    external_id CHARACTER VARYING(256) UNIQUE,
    email CHARACTER VARYING(256),
    real_name CHARACTER VARYING(256),
    active BOOLEAN NOT NULL
);

CREATE TABLE permissions (
    -- resource_type "ResourceType" NOT NULL,
    role_id UUID REFERENCES roles (id) ON DELETE CASCADE NOT NULL,
    permission "Permission" NOT NULL,
    dataset_id UUID REFERENCES datasets (id) ON DELETE CASCADE,
    layer_id UUID REFERENCES layers (id) ON DELETE CASCADE,
    layer_collection_id UUID REFERENCES layer_collections (
        id
    ) ON DELETE CASCADE,
    project_id UUID REFERENCES projects (id) ON DELETE CASCADE,
    CHECK (
        (
            (dataset_id IS NOT NULL)::INTEGER
            + (layer_id IS NOT NULL)::INTEGER
            + (layer_collection_id IS NOT NULL)::INTEGER
            + (project_id IS NOT NULL)::INTEGER
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
);;

CREATE VIEW user_permitted_projects
AS
SELECT
    r.user_id,
    p.project_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.project_id IS NOT NULL
);;

CREATE VIEW user_permitted_layer_collections
AS
SELECT
    r.user_id,
    p.layer_collection_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.layer_collection_id IS NOT NULL
);;

CREATE VIEW user_permitted_layers
AS
SELECT
    r.user_id,
    p.layer_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.layer_id IS NOT NULL
);;
