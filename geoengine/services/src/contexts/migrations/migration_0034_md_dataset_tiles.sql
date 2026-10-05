-- Persist the per-file data of an `MdGdalSource` the same way
-- `MultiBandGdalSource` persists its tiles: in a table of rows, not inside the
-- dataset's meta data.
--
-- One row = one MD array file. The file's time axis is stored twice on purpose:
-- `time_steps` holds one interval per z slice and is what the read path uses,
-- while `time_descriptor` is the compact form the API advertises. Storing both
-- keeps a regular axis readable with a single row lookup and lets the DB keep
-- serving the descriptor even when the slice intervals are not needed.
-- `time` holds the file's overall bounds so that row filtering can keep using
-- `time_interval_intersects`.

-- `ZRole` is derived as `ToSql`/`FromSql` in the operators crate, so it needs a
-- matching enum type now that it is no longer hidden inside a jsonb blob.
CREATE TYPE "ZRole" AS ENUM (
    'Band',
    'Variable'
);

-- Dataset level description of an MD dataset: the result descriptor plus
-- the two properties that are constant across all of its files.
CREATE TYPE "GdalMdMetaData" AS (
    result_descriptor "RasterResultDescriptor",
    z_role "ZRole",
    wrap boolean,
    -- upper bound on how many z slices one GDAL read may request at once
    max_z_batch_size bigint
);

ALTER TYPE "MetaDataDefinition"
ADD ATTRIBUTE gdal_md_meta_data "GdalMdMetaData";

CREATE TABLE dataset_md_tiles (
    id uuid NOT NULL PRIMARY KEY,
    dataset_id uuid NOT NULL,
    time "TimeInterval" NOT NULL, -- noqa: references.keywords
    -- the *presented* footprint (wrap-around aware), like every other bbox here
    bbox "SpatialPartition2D" NOT NULL,
    band oid NOT NULL,
    -- position in the concatenated z axis of its band
    z_index bigint NOT NULL,
    array_name text NOT NULL,
    array_group text,
    -- the compact form of `time_steps`, what the API advertises
    time_descriptor "TimeDescriptor" NOT NULL,
    -- one interval per z slice, always populated
    time_steps "TimeInterval" [] NOT NULL,
    gdal_params "GdalDatasetParameters" NOT NULL
);

CREATE UNIQUE INDEX dataset_md_tiles_unique_idx ON dataset_md_tiles (
    dataset_id,
    time,
    bbox,
    band,
    z_index,
    array_name
);

-- helper type for batch checking MD tile validity
CREATE TYPE "MdTileKey" AS (
    time "TimeInterval",
    bbox "SpatialPartition2D",
    band oid,
    z_index bigint,
    array_name text
);

-- helper type for batch inserting MD tiles
CREATE TYPE "MdTileEntry" AS (
    id uuid,
    dataset_id uuid,
    time "TimeInterval",
    bbox "SpatialPartition2D",
    band oid,
    z_index bigint,
    array_name text,
    array_group text,
    time_descriptor "TimeDescriptor",
    time_steps "TimeInterval" [],
    gdal_params "GdalDatasetParameters"
);
