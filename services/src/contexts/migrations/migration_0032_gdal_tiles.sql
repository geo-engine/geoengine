CREATE TYPE "GdalMultiBand" AS (
    result_descriptor "RasterResultDescriptor"
);

ALTER TYPE "MetaDataDefinition" ADD ATTRIBUTE gdal_multi_band "GdalMultiBand";

CREATE TABLE dataset_tiles (
    dataset_id uuid NOT NULL,
    time "TimeInterval" NOT NULL, -- noqa: references.keywords
    bbox "SpatialPartition2D" NOT NULL,
    band oid NOT NULL,
    z_index oid NOT NULL,
    gdal_params "GdalDatasetParameters" NOT NULL,
    PRIMARY KEY (dataset_id, time, bbox, band, z_index)
);

-- helper type for batch checking tile validity
CREATE TYPE "TileKey" AS (
    time "TimeInterval",
    bbox "SpatialPartition2D",
    band oid,
    z_index oid
);

-- helper type for batch inserting tiles
CREATE TYPE "TileEntry" AS (
    dataset_id uuid,
    time "TimeInterval",
    bbox "SpatialPartition2D",
    band oid,
    z_index oid,
    gdal_params "GdalDatasetParameters"
);
-- Returns true if the partitions have any space in common
CREATE OR REPLACE FUNCTION SPATIAL_PARTITION2D_INTERSECTS(
    a "SpatialPartition2D", b "SpatialPartition2D"
) RETURNS boolean AS $$
SELECT NOT (
    ( (a).lower_right_coordinate.x <= (b).upper_left_coordinate.x ) OR
    ( (a).upper_left_coordinate.x >= (b).lower_right_coordinate.x ) OR
    ( (a).lower_right_coordinate.y >= (b).upper_left_coordinate.y ) OR
    ( (a).upper_left_coordinate.y <= (b).lower_right_coordinate.y )
);
$$ LANGUAGE sql IMMUTABLE;


-- Returns true if two TimeIntervals overlap
CREATE OR REPLACE FUNCTION TIME_INTERVAL_INTERSECTS(
    a "TimeInterval", b "TimeInterval"
) RETURNS boolean AS $$
SELECT
    -- If a is an instant
    ((a).start = (a)."end" AND (a).start >= (b).start AND (a).start < (b)."end")
    OR
    -- If b is an instant
    ((b).start = (b)."end" AND (b).start >= (a).start AND (b).start < (a)."end")
    OR
    -- Regular overlap for intervals
    ((a).start < (b)."end" AND (b).start < (a)."end")
;
$$ LANGUAGE sql IMMUTABLE;
