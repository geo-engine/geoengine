-- A coarser asset with the same band as a STAC dataset band, e.g. its 60 m
-- file of a 10 m dataset (factor 6). The source reads it for overview levels
-- instead of the native file.
CREATE TYPE "StacCoarserAsset" AS (
    factor bigint,
    asset_band "StacAssetBand"
);

-- NULL for existing bands means they have no coarser assets.
ALTER TYPE "StacProviderDatasetBand"
ADD ATTRIBUTE coarser_assets "StacCoarserAsset"[];
