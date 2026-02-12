ALTER TABLE dataset_tiles
ALTER COLUMN z_index TYPE bigint;

-- recreate helper types. 
-- there is no need to migrate data, as they are not used as columns.
DROP TYPE IF EXISTS "TileEntry";
DROP TYPE IF EXISTS "TileKey";

CREATE TYPE "TileKey" AS (
    time "TimeInterval",
    bbox "SpatialPartition2D",
    band oid,
    z_index bigint
);

CREATE TYPE "TileEntry" AS (
    id uuid,
    dataset_id uuid,
    time "TimeInterval",
    bbox "SpatialPartition2D",
    band oid,
    z_index bigint,
    gdal_params "GdalDatasetParameters"
);
