-- `max_overview_level` of a spatial grid descriptor is at least 1 from now on:
-- 1 says that the files of a dataset have no overviews, NULL still means that
-- the levels are unknown. 0 meant the same as 1 and becomes 1.

-- datasets: the result descriptor ...
UPDATE datasets
SET result_descriptor.raster.spatial_grid.max_overview_level = 1
WHERE (result_descriptor).raster.spatial_grid.max_overview_level < 1;

-- ... and its copy in each kind of GDAL meta data
UPDATE datasets
SET
    meta_data.gdal_meta_data_regular.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    = 1
WHERE
    (meta_data).gdal_meta_data_regular.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    < 1;

UPDATE datasets
SET meta_data.gdal_static.result_descriptor.spatial_grid.max_overview_level = 1
WHERE
    (meta_data).gdal_static.result_descriptor.spatial_grid.max_overview_level
    < 1;

UPDATE datasets
SET
    meta_data.gdal_metadata_net_cdf_cf.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    = 1
WHERE
    (meta_data).gdal_metadata_net_cdf_cf.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    < 1;

UPDATE datasets
SET
    meta_data.gdal_meta_data_list.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    = 1
WHERE
    (meta_data).gdal_meta_data_list.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    < 1;

UPDATE datasets
SET
    meta_data.gdal_multi_band.result_descriptor.spatial_grid.max_overview_level
    = 1
WHERE
    (meta_data).gdal_multi_band.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    < 1;

-- the datasets of STAC providers
CREATE FUNCTION pg_temp.with_non_zero_max_overview_levels(
    datasets "StacProviderDataset"[]
) RETURNS "StacProviderDataset"[] AS $$
DECLARE
    dataset "StacProviderDataset";
    migrated "StacProviderDataset" [] := '{}';
BEGIN
    FOREACH dataset IN ARRAY datasets LOOP
        IF (dataset.spatial_grid).max_overview_level < 1 THEN
            dataset.spatial_grid.max_overview_level := 1;
        END IF;
        migrated := migrated || dataset;
    END LOOP;
    RETURN migrated;
END;
$$ LANGUAGE plpgsql;

UPDATE layer_providers
SET
    definition.stac_data_provider_definition.datasets
    = pg_temp.with_non_zero_max_overview_levels(
        (definition).stac_data_provider_definition.datasets
    )
WHERE (definition).stac_data_provider_definition.datasets IS NOT NULL;

DROP FUNCTION pg_temp.with_non_zero_max_overview_levels;
