CREATE TYPE "GdalMultiBand" AS (
    result_descriptor "RasterResultDescriptor"
);

ALTER TYPE "MetaDataDefinition" ADD ATTRIBUTE gdal_multi_band "GdalMultiBand";

CREATE TABLE dataset_tiles (
    dataset_id uuid NOT NULL,
    time "TimeInterval" NOT NULL,
    bbox "SpatialPartition2D" NOT NULL,
    band OID NOT NULL,
    z_index OID NOT NULL,
    gdal_params "GdalDatasetParameters" NOT NULL,
    PRIMARY KEY (dataset_id, time, bbox, band, z_index)
);

-- Returns true if the `other` partition has any space in common with the partition  
CREATE OR REPLACE FUNCTION spatial_partition2d_intersects(a "SpatialPartition2D", b "SpatialPartition2D") RETURNS boolean AS $$
SELECT NOT (
    ( (a).lower_right_coordinate.x <= (b).upper_left_coordinate.x ) OR
    ( (a).upper_left_coordinate.x >= (b).lower_right_coordinate.x ) OR
    ( (a).lower_right_coordinate.y >= (b).upper_left_coordinate.y ) OR
    ( (a).upper_left_coordinate.y <= (b).lower_right_coordinate.y )
);
$$ LANGUAGE SQL IMMUTABLE;


-- Returns true if two TimeIntervals overlap
CREATE OR REPLACE FUNCTION time_interval_intersects(a "TimeInterval", b "TimeInterval") RETURNS boolean AS $$
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
$$ LANGUAGE SQL IMMUTABLE;
