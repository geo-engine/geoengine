-- `NULL` keeps the previous behavior:
-- items belong to the time step of their `datetime`
CREATE TYPE "StacItemTime" AS ENUM (
    'Datetime',
    'Range'
);

-- `NULL` uses the default limit of parallel asset header probes
ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE item_time "StacItemTime",
ADD ATTRIBUTE max_concurrent_asset_probes int;
