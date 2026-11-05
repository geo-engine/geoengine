-- `NULL` uses the default limit, so existing providers keep their behavior
ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE max_concurrent_cell_searches int;
