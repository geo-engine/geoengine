-- add the new types
CREATE TYPE "GridBoundingBox2D" AS (
    y_min bigint,
    y_max bigint,
    x_min bigint,
    x_max bigint
);
CREATE TYPE "GeoTransform" AS (
    origin_coordinate "Coordinate2D",
    x_pixel_size double precision,
    y_pixel_size double precision
);
CREATE TYPE "SpatialGridDefinition" AS (
    geo_transform "GeoTransform",
    grid_bounds "GridBoundingBox2D"
);
CREATE TYPE "SpatialGridDescriptorState" AS ENUM ('Source', 'Merged');
CREATE TYPE "SpatialGridDescriptor" AS (
    "state" "SpatialGridDescriptorState",
    spatial_grid "SpatialGridDefinition"
);
-- adapt the RasterResultDescriptor --> add the new attribute
ALTER TYPE "RasterResultDescriptor"
ADD ATTRIBUTE spatial_grid "SpatialGridDescriptor";
-- migrate gdal_static metadata
WITH cte AS (
    SELECT
        id,
        (meta_data).gdal_static AS meta
    FROM datasets
    WHERE (meta_data).gdal_static IS NOT NULL
)

UPDATE datasets
SET
    result_descriptor.raster.spatial_grid = (
        'Source',
        (
            (
                (cte).meta.params.geo_transform.origin_coordinate,
                (cte).meta.params.geo_transform.x_pixel_size,
                (cte).meta.params.geo_transform.x_pixel_size
            )::"GeoTransform",
            (
                0,
                (cte).meta.params.height - 1,
                0,
                (cte).meta.params.width - 1
            )::"GridBoundingBox2D"
        )::"SpatialGridDefinition"
    )::"SpatialGridDescriptor"
FROM cte
WHERE datasets.id = cte.id;
-- migrate gdal_regular metadata
WITH cte AS (
    SELECT
        id,
        (meta_data).gdal_meta_data_regular AS meta
    FROM datasets
    WHERE (meta_data).gdal_meta_data_regular IS NOT NULL
)

UPDATE datasets
SET
    result_descriptor.raster.spatial_grid = (
        'Source',
        (
            (
                (cte).meta.params.geo_transform.origin_coordinate,
                (cte).meta.params.geo_transform.x_pixel_size,
                (cte).meta.params.geo_transform.x_pixel_size
            )::"GeoTransform",
            (
                0,
                (cte).meta.params.height - 1,
                0,
                (cte).meta.params.width - 1
            )::"GridBoundingBox2D"
        )::"SpatialGridDefinition"
    )::"SpatialGridDescriptor"
FROM cte
WHERE datasets.id = cte.id;
-- migrate gdal_metadata_net_cdf_cf
WITH cte AS (
    SELECT
        id,
        (meta_data).gdal_metadata_net_cdf_cf AS meta
    FROM datasets
    WHERE (meta_data).gdal_metadata_net_cdf_cf IS NOT NULL
)

UPDATE datasets
SET
    result_descriptor.raster.spatial_grid = (
        'Source',
        (
            (
                (cte).meta.params.geo_transform.origin_coordinate,
                (cte).meta.params.geo_transform.x_pixel_size,
                (cte).meta.params.geo_transform.x_pixel_size
            )::"GeoTransform",
            (
                0,
                (cte).meta.params.height - 1,
                0,
                (cte).meta.params.width - 1
            )::"GridBoundingBox2D"
        )::"SpatialGridDefinition"
    )::"SpatialGridDescriptor"
FROM cte
WHERE datasets.id = cte.id;
-- migrate gdal_metadata_lsit
CREATE FUNCTION pg_temp.spatial_grid_def_from_params_array(
    t_slices "GdalLoadingInfoTemporalSlice" []
) RETURNS "SpatialGridDefinition" AS $$
DECLARE b_size_x double precision;
b_size_y double precision;
b_ul_x double precision;
b_ul_y double precision;
b_lr_x double precision;
b_lr_y double precision;
t_x double precision;
t_y double precision;
n "SpatialGridDefinition";
t "GdalLoadingInfoTemporalSlice";
BEGIN FOREACH t IN ARRAY t_slices LOOP IF t.params IS NULL THEN CONTINUE;
END IF;
t_x := (t).params.geo_transform.origin_coordinate.x + (t).params.geo_transform.x_pixel_size * (t).params.width;
t_y := (t).params.geo_transform.origin_coordinate.y + (t).params.geo_transform.y_pixel_size * (t).params.height;
IF b_size_x IS NULL THEN b_size_x := (t).params.geo_transform.x_pixel_size;
b_size_y := (t).params.geo_transform.y_pixel_size;
b_ul_x := (t).params.geo_transform.origin_coordinate.x;
b_ul_y := (t).params.geo_transform.origin_coordinate.y;
b_lr_x := t_x;
b_lr_y := t_y;
END IF;
b_ul_x := LEAST(
    b_ul_x,
    (t).params.geo_transform.origin_coordinate.x
);
b_ul_y := GREATEST(
    b_ul_y,
    (t).params.geo_transform.origin_coordinate.y
);
b_lr_x := GREATEST(b_lr_x, t_x);
b_lr_y := LEAST(b_lr_y, t_y);
END LOOP;
RETURN (
    (
        (b_ul_x, b_ul_y)::"Coordinate2D",
        b_size_x,
        b_size_y
    )::"GeoTransform",
    (
        0,
        ((b_ul_y - b_lr_y) / b_size_y) -1,
        0,
        ((b_lr_x - b_ul_x) / b_size_x) -1
    )::"GridBoundingBox2D"
)::"SpatialGridDefinition";
END;
$$ LANGUAGE plpgsql;
WITH cte AS (
    SELECT
        id,
        (meta_data).gdal_meta_data_list AS meta
    FROM datasets
    WHERE (meta_data).gdal_meta_data_list IS NOT NULL
)
UPDATE datasets
SET
    result_descriptor.raster.spatial_grid = (
        'Source',
        pg_temp.spatial_grid_def_from_params_array(((cte).meta.params))
    )::"SpatialGridDescriptor"
FROM cte
WHERE datasets.id = cte.id;
-- remove the old attributes
ALTER TYPE "RasterResultDescriptor" DROP ATTRIBUTE bbox;
ALTER TYPE "RasterResultDescriptor" DROP ATTRIBUTE resolution;
-- mark the spatial_grid as NOT NULL
DROP FUNCTION IF EXISTS pg_temp.spatial_grid_def_from_params_array;
