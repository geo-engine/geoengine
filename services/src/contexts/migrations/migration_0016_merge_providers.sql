ALTER TYPE "DataProviderDefinition" ADD ATTRIBUTE
sentinel_s2_l2_a_cogs_provider_definition
"SentinelS2L2ACogsProviderDefinition";

ALTER TYPE "DataProviderDefinition" ADD ATTRIBUTE
copernicus_dataspace_provider_definition
"CopernicusDataspaceDataProviderDefinition";

INSERT INTO layer_providers (
    id,
    type_name,
    name,
    definition
) SELECT
    pro.id,
    pro.type_name,
    pro.name,
    (
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL, -- noqa: PRS
        (pro.definition).sentinel_s2_l2_a_cogs_provider_definition,
        NULL
    )::"DataProviderDefinition"
FROM pro_layer_providers AS pro
WHERE pro.type_name = 'SentinelS2L2ACogsProviderDefinition';

INSERT INTO layer_providers (
    id,
    type_name,
    name,
    definition
) SELECT
    pro.id,
    pro.type_name,
    pro.name,
    (
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL, -- noqa: PRS
        (pro.definition).copernicus_dataspace_provider_definition
    )::"DataProviderDefinition"
FROM pro_layer_providers AS pro
WHERE pro.type_name = 'CopernicusDataspaceDataProviderDefinition';

DROP TABLE pro_layer_providers;
DROP TYPE "ProDataProviderDefinition";

-- user_sessions

ALTER TABLE sessions ADD COLUMN
user_id uuid REFERENCES users (id) ON DELETE CASCADE;
ALTER TABLE sessions ADD COLUMN created timestamp with time zone;
ALTER TABLE sessions ADD COLUMN valid_until timestamp with time zone;

UPDATE sessions SET
    user_id = us.user_id,
    created = us.created,
    valid_until = us.valid_until
FROM user_sessions AS us
WHERE sessions.id = us.session_id;

ALTER TABLE sessions ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE sessions ALTER COLUMN created SET NOT NULL;
ALTER TABLE sessions ALTER COLUMN valid_until SET NOT NULL;

DROP TABLE user_sessions;
