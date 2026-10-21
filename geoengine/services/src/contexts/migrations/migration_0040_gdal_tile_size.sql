-- optional tile size override, `NULL` means the configured default
CREATE TYPE "TileSize" AS (
    y bigint,
    x bigint
);

ALTER TYPE "GdalDatasetParameters"
ADD ATTRIBUTE tile_size "TileSize";
