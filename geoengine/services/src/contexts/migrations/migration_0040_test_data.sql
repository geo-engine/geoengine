-- one dataset per place that holds a spatial grid descriptor, each with
-- max_overview_level 0, and one dataset with levels that must stay
INSERT INTO datasets (
    id, name, display_name, description, source_operator, result_descriptor,
    meta_data
)
SELECT
    d.id::uuid AS id,
    ROW(NULL, d.display_name)::"DatasetName" AS name,
    d.display_name,
    d.display_name AS description,
    'GdalSource' AS source_operator,
    ROW(NULL, NULL, NULL)::"ResultDescriptor" AS result_descriptor,
    ROW(NULL, NULL, NULL, NULL, NULL, NULL, NULL)::"MetaDataDefinition"
        AS meta_data
FROM (
    VALUES
    ('00000000-0000-0000-0000-000000000001', 'zero'),
    ('00000000-0000-0000-0000-000000000002', 'kept')
) AS d (id, display_name);

UPDATE datasets
SET
    result_descriptor.raster.spatial_grid.max_overview_level = 0,
    meta_data.gdal_meta_data_regular.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    = 0,
    meta_data.gdal_static.result_descriptor.spatial_grid.max_overview_level = 0,
    meta_data.gdal_metadata_net_cdf_cf.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    = 0,
    meta_data.gdal_meta_data_list.result_descriptor.spatial_grid.max_overview_level -- noqa: LT05
    = 0,
    meta_data.gdal_multi_band.result_descriptor.spatial_grid.max_overview_level
    = 0
WHERE display_name = 'zero';

UPDATE datasets
SET
    result_descriptor.raster.spatial_grid.max_overview_level = 8,
    meta_data.gdal_static.result_descriptor.spatial_grid.max_overview_level = 1
WHERE display_name = 'kept';

-- a STAC provider with three datasets: level 0, level 4 and unknown levels
INSERT INTO layer_providers (id, type_name, name, definition)
VALUES (
    '00000000-0000-0000-0000-000000000003',
    'Stac',
    'stac',
    ROW(
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    )::"DataProviderDefinition"
);

UPDATE layer_providers
SET
    definition.stac_data_provider_definition.datasets = ARRAY[
        ROW('a', NULL, NULL, NULL, NULL, NULL, NULL)::"StacProviderDataset",
        ROW('b', NULL, NULL, NULL, NULL, NULL, NULL)::"StacProviderDataset",
        ROW('c', NULL, NULL, NULL, NULL, NULL, NULL)::"StacProviderDataset"
    ];

UPDATE layer_providers -- noqa: PRS
SET
    definition.stac_data_provider_definition.datasets[1].spatial_grid.max_overview_level -- noqa: LT05
    = 0,
    definition.stac_data_provider_definition.datasets[2].spatial_grid.max_overview_level -- noqa: LT05
    = 4;
