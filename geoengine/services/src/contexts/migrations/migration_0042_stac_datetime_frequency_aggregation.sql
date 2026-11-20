-- `NULL` means that the server is not asked for aggregations, so existing
-- providers search
ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE datetime_frequency_aggregation boolean;
