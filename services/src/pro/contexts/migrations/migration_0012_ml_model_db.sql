ALTER TABLE permissions
ADD COLUMN ml_model_id uuid REFERENCES ml_models (id) ON DELETE CASCADE;

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
    ) = 1
);


CREATE VIEW user_permitted_ml_models
AS
SELECT
    r.user_id,
    p.ml_model_id,
    p.permission
FROM user_roles AS r
INNER JOIN permissions AS p ON (
    r.role_id = p.role_id AND p.ml_model_id IS NOT NULL
);
