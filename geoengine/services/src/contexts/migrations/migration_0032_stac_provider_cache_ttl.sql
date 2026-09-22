-- Nullable values inherit the configured global cache default. A stored zero
-- explicitly disables caching for the provider.
ALTER TYPE "StacDataProviderDefinition" ADD ATTRIBUTE cache_ttl_secs int;
