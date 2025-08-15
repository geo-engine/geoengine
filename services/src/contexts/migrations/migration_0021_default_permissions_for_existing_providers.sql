CREATE TEMP TABLE skip AS
SELECT provider_id AS skip_id
FROM permissions
WHERE provider_id IS NOT NULL;

INSERT INTO permissions (role_id, permission, provider_id)
SELECT
    'd5328854-6190-4af9-ad69-4e74b0961ac9' AS role_id,
    'Owner' AS permission,
    id AS provider_id
FROM layer_providers
WHERE NOT EXISTS (
    SELECT skip_id FROM skip
    WHERE id = skip_id
);

INSERT INTO permissions (role_id, permission, provider_id)
SELECT
    '4e8081b6-8aa6-4275-af0c-2fa2da557d28' AS role_id,
    'Read' AS permission,
    id AS provider_id
FROM layer_providers
WHERE NOT EXISTS (
    SELECT skip_id FROM skip
    WHERE id = skip_id
);

INSERT INTO permissions (role_id, permission, provider_id)
SELECT
    'fd8e87bf-515c-4f36-8da6-1a53702ff102' AS role_id,
    'Read' AS permission,
    id AS provider_id
FROM layer_providers
WHERE NOT EXISTS (
    SELECT skip_id FROM skip
    WHERE id = skip_id
);

DROP TABLE skip;
