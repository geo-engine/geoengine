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

CREATE TYPE "StacAssetBand" AS (
    asset_title text,
    band_name text
);

ALTER TYPE "StacProviderDatasetBand" ADD ATTRIBUTE asset_band "StacAssetBand";
ALTER TYPE "StacProviderDatasetBand" ADD ATTRIBUTE band_descriptor "RasterBandDescriptor";

-- Migrate existing stored STAC provider definitions: bundle the flat
-- `asset_title`/`band_name` attributes into the new `asset_band` attribute.
CREATE FUNCTION pg_temp.stac_migrate_bands(
    bands "StacProviderDatasetBand" []
) RETURNS "StacProviderDatasetBand" [] AS $$
DECLARE
    b "StacProviderDatasetBand";
    new_bands "StacProviderDatasetBand" [] := '{}';
BEGIN
    FOREACH b IN ARRAY bands LOOP
        new_bands := array_append(
            new_bands,
            ROW(
                NULL,
                NULL,
                ROW((b).asset_title, (b).band_name)::"StacAssetBand",
                ROW(
                    COALESCE((b).band_name, (b).asset_title),
                    ROW(NULL, NULL)::"Measurement"
                )::"RasterBandDescriptor"
            )::"StacProviderDatasetBand"
        );
    END LOOP;
    RETURN new_bands;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_temp.stac_migrate_provider_def(
    def "StacDataProviderDefinition"
) RETURNS "StacDataProviderDefinition" AS $$
DECLARE
    d "StacProviderDataset";
    new_datasets "StacProviderDataset" [] := '{}';
BEGIN
    FOREACH d IN ARRAY (def).datasets LOOP
        new_datasets := array_append(
            new_datasets,
            ROW(
                (d).name,
                (d).description,
                (d).data_type,
                (d).resolution,
                (d).projection,
                (d).spatial_grid,
                pg_temp.stac_migrate_bands((d).bands)
            )::"StacProviderDataset"
        );
    END LOOP;
    RETURN ROW(
        (def).name,
        (def).id,
        (def).description,
        (def).priority,
        (def).api_url,
        (def).collection_name,
        (def).s3_config,
        (def).time_dimension,
        new_datasets,
        (def).page_limit,
        (def).query_timeout_secs
    )::"StacDataProviderDefinition";
END;
$$ LANGUAGE plpgsql;

UPDATE layer_providers
SET definition = ROW(
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    pg_temp.stac_migrate_provider_def((definition).stac_data_provider_definition)
)::"DataProviderDefinition"
WHERE (definition).stac_data_provider_definition IS NOT NULL;

-- Drop the now-redundant flat addressing attributes.
ALTER TYPE "StacProviderDatasetBand" DROP ATTRIBUTE asset_title;
ALTER TYPE "StacProviderDatasetBand" DROP ATTRIBUTE band_name;

DROP FUNCTION pg_temp.stac_migrate_bands;
DROP FUNCTION pg_temp.stac_migrate_provider_def;
