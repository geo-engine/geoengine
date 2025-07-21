ALTER TABLE permissions
ADD COLUMN provider_id uuid
REFERENCES layer_providers (id) ON DELETE CASCADE;

ALTER TABLE permissions
DROP CONSTRAINT permissions_check;

ALTER TABLE permissions
ADD CONSTRAINT permissions_check CHECK (
    (
        (dataset_id IS NOT NULL)::integer
        + (layer_id IS NOT NULL)::integer
        + (layer_collection_id IS NOT NULL)::integer
        + (project_id IS NOT NULL)::integer
        + (ml_model_id IS NOT NULL)::integer
        + (provider_id IS NOT NULL)::integer
    ) = 1
);


CREATE VIEW user_permitted_providers
AS
SELECT
    r.user_id,
    p.provider_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.provider_id IS NOT NULL
);
