ALTER TYPE "EbvPortalDataProviderDefinition" RENAME ATTRIBUTE "path" TO "data";
ALTER TYPE "NetCdfCfDataProviderDefinition" RENAME ATTRIBUTE "path" TO "data";

ALTER TYPE "EbvPortalDataProviderDefinition"
ADD ATTRIBUTE metadata_db_config "DatabaseConnectionConfig";
ALTER TYPE "NetCdfCfDataProviderDefinition"
ADD ATTRIBUTE metadata_db_config "DatabaseConnectionConfig";

-- we cannot guess a database config, so we have to remove the invalid providers
DELETE FROM layer_providers
WHERE
    NOT ((definition).ebv_portal_data_provider_definition IS NULL)
    OR NOT ((definition).net_cdf_cf_data_provider_definition IS NULL);
