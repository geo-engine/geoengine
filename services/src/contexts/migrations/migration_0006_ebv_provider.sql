ALTER TYPE "EbvPortalDataProviderDefinition" RENAME ATTRIBUTE "path" TO "data";
ALTER TYPE "NetCdfCfDataProviderDefinition" RENAME ATTRIBUTE "path" TO "data";

ALTER TYPE "EbvPortalDataProviderDefinition"
ADD ATTRIBUTE metadata_db_config "DatabaseConnectionConfig";
ALTER TYPE "NetCdfCfDataProviderDefinition"
ADD ATTRIBUTE metadata_db_config "DatabaseConnectionConfig";
