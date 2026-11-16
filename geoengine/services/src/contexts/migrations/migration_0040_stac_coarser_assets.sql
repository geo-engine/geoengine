-- A coarser asset with the same band as a STAC dataset band, e.g. its 60 m
-- file. The source reads it for overview levels instead of the native file.
CREATE TYPE "StacCoarserAsset" AS (
    resolution "SpatialResolution",
    asset_band "StacAssetBand"
);

-- NULL for existing bands means they have no coarser assets.
ALTER TYPE "StacProviderDatasetBand"
ADD ATTRIBUTE coarser_assets "StacCoarserAsset"[];
