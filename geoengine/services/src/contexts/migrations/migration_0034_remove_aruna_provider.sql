DELETE FROM layer_providers
WHERE (definition).aruna_data_provider_definition IS NOT NULL;

ALTER TYPE "DataProviderDefinition"
DROP ATTRIBUTE aruna_data_provider_definition RESTRICT;

DROP TYPE "ArunaDataProviderDefinition" RESTRICT;