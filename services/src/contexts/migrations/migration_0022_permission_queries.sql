WITH duplicates AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY role_id, permission, provider_id
            ORDER BY ctid
        ) AS rn
    FROM permissions
)

DELETE FROM permissions
WHERE ctid IN (
    SELECT ctid FROM duplicates
    WHERE rn > 1
);

CREATE UNIQUE INDEX ON permissions (
    role_id,
    permission,
    provider_id
);

ALTER VIEW user_permitted_datasets RENAME COLUMN permission TO max_permission;

CREATE OR REPLACE VIEW user_permitted_datasets
AS
SELECT
    r.user_id,
    p.dataset_id,
    MAX(p.permission) AS max_permission
FROM user_roles AS r
INNER JOIN permissions AS p
    ON (
        r.role_id = p.role_id AND p.dataset_id IS NOT NULL
    )
GROUP BY r.user_id, p.dataset_id;

ALTER VIEW user_permitted_projects RENAME COLUMN permission TO max_permission;

CREATE OR REPLACE VIEW user_permitted_projects
AS
SELECT
    r.user_id,
    p.project_id,
    MAX(p.permission) AS max_permission
FROM user_roles AS r
INNER JOIN permissions AS p
    ON (
        r.role_id = p.role_id AND p.project_id IS NOT NULL
    )
GROUP BY r.user_id, p.project_id;

ALTER VIEW user_permitted_layer_collections
RENAME COLUMN permission TO max_permission;

CREATE OR REPLACE VIEW user_permitted_layer_collections
AS
SELECT
    r.user_id,
    p.layer_collection_id,
    MAX(p.permission) AS max_permission
FROM user_roles AS r
INNER JOIN permissions AS p
    ON (
        r.role_id = p.role_id AND p.layer_collection_id IS NOT NULL
    )
GROUP BY r.user_id, p.layer_collection_id;

ALTER VIEW user_permitted_layers RENAME COLUMN permission TO max_permission;

CREATE OR REPLACE VIEW user_permitted_layers
AS
SELECT
    r.user_id,
    p.layer_id,
    MAX(p.permission) AS max_permission
FROM user_roles AS r
INNER JOIN permissions AS p
    ON (
        r.role_id = p.role_id AND p.layer_id IS NOT NULL
    )
GROUP BY r.user_id, p.layer_id;

ALTER VIEW user_permitted_providers RENAME COLUMN permission TO max_permission;

CREATE OR REPLACE VIEW user_permitted_providers
AS
SELECT
    r.user_id,
    p.provider_id,
    MAX(p.permission) AS max_permission
FROM user_roles AS r
INNER JOIN permissions AS p
    ON (
        r.role_id = p.role_id AND p.provider_id IS NOT NULL
    )
GROUP BY r.user_id, p.provider_id;

ALTER VIEW user_permitted_ml_models RENAME COLUMN permission TO max_permission;

CREATE OR REPLACE VIEW user_permitted_ml_models
AS
SELECT
    r.user_id,
    p.ml_model_id,
    MAX(p.permission) AS max_permission
FROM user_roles AS r
INNER JOIN permissions AS p
    ON (
        r.role_id = p.role_id AND p.ml_model_id IS NOT NULL
    )
GROUP BY r.user_id, p.ml_model_id;
