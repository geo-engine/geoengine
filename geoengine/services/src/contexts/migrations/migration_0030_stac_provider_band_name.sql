-- Bundle the band *addressing* fields into a nested `StacAssetBand` type and
-- add a `band_descriptor` (`RasterBandDescriptor`) for the band in the
-- resulting geo engine dataset layer.
--
-- Before: "StacProviderDatasetBand" (asset_title text, band_name text)
-- After:  "StacAssetBand" (asset_title text, band_name text)
--         "StacProviderDatasetBand" (asset_band "StacAssetBand",
--                                     band_descriptor "RasterBandDescriptor")
--
-- `asset_title`/`band_name` *address* a band inside the STAC asset files (which
-- asset file, and which raster channel within it). `band_descriptor` is the
-- `RasterBandDescriptor` of the geo engine dataset layer band, populated with
-- the naming fallback ("use band_name, then asset_title") and a unitless
-- measurement.
--
-- Additionally, this migration takes over the `page_limit` attribute of
-- `StacDataProviderDefinition` that previously lived in the released migration
-- `0028_stac_provider`, so that migration stays untouched.

CREATE TYPE "StacAssetBand" AS (
    asset_title text,
    band_name text
);

ALTER TYPE "StacProviderDatasetBand" ADD ATTRIBUTE asset_band "StacAssetBand";
ALTER TYPE "StacProviderDatasetBand"
ADD ATTRIBUTE band_descriptor "RasterBandDescriptor";

-- `page_limit` was moved here from migration 0028, which is already released.
-- It must be added before the data migration below, since
-- `pg_temp.stac_migrate_provider_def` reads `(def).page_limit`.
ALTER TYPE "StacDataProviderDefinition" ADD ATTRIBUTE page_limit bigint;

-- Drop the now-redundant flat addressing attributes.
ALTER TYPE "StacProviderDatasetBand" DROP ATTRIBUTE asset_title;
ALTER TYPE "StacProviderDatasetBand" DROP ATTRIBUTE band_name;
