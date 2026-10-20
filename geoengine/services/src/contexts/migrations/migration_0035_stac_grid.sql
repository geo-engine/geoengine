CREATE TYPE "StacGrid" AS (
    target_number_of_cells integer
);

ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE stac_grid "StacGrid";
