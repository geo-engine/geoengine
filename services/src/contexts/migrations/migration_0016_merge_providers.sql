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

DROP TABLE pro_layer_providers;
DROP TYPE "ProDataProviderDefinition";
