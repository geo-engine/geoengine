CREATE TYPE "RegularTimeDimension" AS (
    origin bigint,
    step "TimeStep"
);

CREATE TYPE "TimeDimensionDiscriminator" AS ENUM (
    'Regular',
    'Irregular'
);

CREATE TYPE "TimeDimension" AS (
    regular_dimension "RegularTimeDimension",
    discriminant "TimeDimensionDiscriminator"
);

CREATE TYPE "TimeDescriptor" AS (
    bounds "TimeInterval",
    dimension "TimeDimension"
);

ALTER TYPE "RasterResultDescriptor" RENAME ATTRIBUTE time TO old_time;
ALTER TYPE "RasterResultDescriptor" ADD ATTRIBUTE time "TimeDescriptor";

WITH cte AS (
    SELECT
        id,
        (result_descriptor).raster.old_time AS old_time
    FROM datasets
    WHERE (result_descriptor).raster IS NOT NULL
)
UPDATE datasets
SET
    result_descriptor.raster.time = (
        cte.old_time,
        (NULL, 'Irregular')::"TimeDimension"
    )::"TimeDescriptor"
FROM cte
WHERE datasets.id = cte.id;

WITH cte AS (
    SELECT
        id,
        (result_descriptor).raster.old_time AS old_time,
        (meta_data).gdal_meta_data_regular AS meta
    FROM datasets
    WHERE (meta_data).gdal_meta_data_regular IS NOT NULL
)
UPDATE datasets
SET
    result_descriptor.raster.time = (
        cte.old_time,
        (
            (
                (cte).meta.data_time.start,
                (cte).meta.step
            )::"RegularTimeDimension",
            'Regular'
        )::"TimeDimension"
    )::"TimeDescriptor"
FROM cte
WHERE datasets.id = cte.id;

ALTER TYPE "RasterResultDescriptor" DROP ATTRIBUTE old_time;
