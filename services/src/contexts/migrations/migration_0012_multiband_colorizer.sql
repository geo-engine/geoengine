-- add 'MultiBand' variant for raster colorizer

ALTER TYPE "RasterColorizerType"
ADD VALUE 'MultiBand';

-- remove 'Rgba' variant from colorizer

ALTER TYPE "ColorizerType"
RENAME TO "ColorizerType_old";

CREATE TYPE "ColorizerType" AS ENUM (
    'LinearGradient', 'LogarithmicGradient', 'Palette'
);


ALTER TYPE "Colorizer"
RENAME TO "Colorizer_old";

CREATE TYPE "Colorizer" AS (
    "type" "ColorizerType",
    -- linear/logarithmic gradient
    breakpoints "Breakpoint" [],
    no_data_color "RgbaColor",
    over_color "RgbaColor",
    under_color "RgbaColor",
    -- palette
    -- (colors --> breakpoints)
    default_color "RgbaColor"
    -- (no_data_color)
    -- rgba
    -- (nothing)
);


ALTER TYPE "RasterColorizer"
RENAME TO "RasterColorizer_old";

CREATE TYPE "RasterColorizer" AS (
    "type" "RasterColorizerType",
    -- single band colorizer
    band bigint,
    band_colorizer "Colorizer",
    -- multi band colorizer
    red_band bigint,
    red_min double precision,
    red_max double precision,
    red_scale double precision,
    green_band bigint,
    green_min double precision,
    green_max double precision,
    green_scale double precision,
    blue_band bigint,
    blue_min double precision,
    blue_max double precision,
    blue_scale double precision
);


ALTER TYPE "RasterSymbology"
RENAME TO "RasterSymbology_old";

CREATE TYPE "RasterSymbology" AS (
    opacity double precision,
    raster_colorizer "RasterColorizer"
);


ALTER TYPE "ColorParam"
RENAME TO "ColorParam_old";

CREATE TYPE "ColorParam" AS (
    -- static
    color "RgbaColor",
    -- derived
    attribute text,
    colorizer "Colorizer"
);


ALTER TYPE "StrokeParam"
RENAME TO "StrokeParam_old";

CREATE TYPE "StrokeParam" AS (
    width "NumberParam",
    color "ColorParam"
);


ALTER TYPE "TextSymbology"
RENAME TO "TextSymbology_old";

CREATE TYPE "TextSymbology" AS (
    attribute text,
    fill_color "ColorParam",
    stroke "StrokeParam"
);


ALTER TYPE "PointSymbology"
RENAME TO "PointSymbology_old";

CREATE TYPE "PointSymbology" AS (
    radius "NumberParam",
    fill_color "ColorParam",
    stroke "StrokeParam",
    text "TextSymbology"
);


ALTER TYPE "LineSymbology"
RENAME TO "LineSymbology_old";

CREATE TYPE "LineSymbology" AS (
    stroke "StrokeParam",
    text "TextSymbology",
    auto_simplified boolean
);


ALTER TYPE "PolygonSymbology"
RENAME TO "PolygonSymbology_old";

CREATE TYPE "PolygonSymbology" AS (
    fill_color "ColorParam",
    stroke "StrokeParam",
    text "TextSymbology",
    auto_simplified boolean
);


ALTER TYPE "Symbology"
RENAME TO "Symbology_old";

CREATE TYPE "Symbology" AS (
    "raster" "RasterSymbology",
    "point" "PointSymbology",
    "line" "LineSymbology",
    "polygon" "PolygonSymbology"
);

-- switch columns to new types

CREATE FUNCTION pg_temp.migrate_colorizer(old "Colorizer_old")
RETURNS "Colorizer"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN CASE -- noqa: PRS
    WHEN old."type" = 'Rgba' -- noqa: PRS
        THEN (
            'LinearGradient'::"ColorizerType", -- noqa: PRS
            array[
                (
                    0.0, 
                    array[128,128,128,255]::"RgbaColor"
                )::"Breakpoint"
            ]::"Breakpoint"[],
            array[0,0,0,0]::"RgbaColor",
            array[0,0,0,0]::"RgbaColor",
            array[0,0,0,0]::"RgbaColor",
            NULL
        )::"Colorizer"
    ELSE old::text::"Colorizer"
END;

CREATE FUNCTION pg_temp.migrate_color_param(old "ColorParam_old")
RETURNS "ColorParam"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN (
    old.color,
    old.attribute,
    pg_temp.migrate_colorizer(old.colorizer)
)::"ColorParam";

CREATE FUNCTION pg_temp.migrate_stroke_param(old "StrokeParam_old")
RETURNS "StrokeParam"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN (
    old.width,
    pg_temp.migrate_color_param(old.color)
)::"StrokeParam";

CREATE FUNCTION pg_temp.migrate_text_symbology(old "TextSymbology_old")
RETURNS "TextSymbology"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN (
    old.attribute,
    pg_temp.migrate_color_param(old.fill_color),
    pg_temp.migrate_stroke_param(old.stroke)
)::"TextSymbology";

CREATE FUNCTION pg_temp.migrate_raster_symbology(old "RasterSymbology_old")
RETURNS "RasterSymbology"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN (
    (old).opacity, -- noqa: PRS
    (
        (old).raster_colorizer."type",
        (old).raster_colorizer.band,
        pg_temp.migrate_colorizer((old).raster_colorizer.band_colorizer),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    )::"RasterColorizer"
)::"RasterSymbology";

CREATE FUNCTION pg_temp.migrate_point_symbology(old "PointSymbology_old")
RETURNS "PointSymbology"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN (
    old.radius,
    pg_temp.migrate_color_param(old.fill_color),
    pg_temp.migrate_stroke_param(old.stroke),
    pg_temp.migrate_text_symbology(old.text)
)::"PointSymbology";

CREATE FUNCTION pg_temp.migrate_line_symbology(old "LineSymbology_old")
RETURNS "LineSymbology"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN (
    pg_temp.migrate_stroke_param(old.stroke),
    pg_temp.migrate_text_symbology(old.text),
    old.auto_simplified
)::"LineSymbology";

CREATE FUNCTION pg_temp.migrate_polygon_symbology(old "PolygonSymbology_old")
RETURNS "PolygonSymbology"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN (
    pg_temp.migrate_color_param(old.fill_color),
    pg_temp.migrate_stroke_param(old.stroke),
    pg_temp.migrate_text_symbology(old.text),
    old.auto_simplified
)::"PolygonSymbology";

CREATE FUNCTION pg_temp.migrate_symbology(old "Symbology_old")
RETURNS "Symbology"
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
RETURN (
    pg_temp.migrate_raster_symbology(old.raster),
    pg_temp.migrate_point_symbology(old.point),
    pg_temp.migrate_line_symbology(old.line),
    pg_temp.migrate_polygon_symbology(old.polygon)
)::"Symbology";

ALTER TABLE datasets
ALTER COLUMN symbology
SET DATA TYPE "Symbology"
USING pg_temp.migrate_symbology(symbology);

ALTER TABLE layers
ALTER COLUMN symbology
SET DATA TYPE "Symbology"
USING pg_temp.migrate_symbology(symbology);

ALTER TABLE project_version_layers
ALTER COLUMN symbology
SET DATA TYPE "Symbology"
USING pg_temp.migrate_symbology(symbology);

ALTER TABLE ebv_provider_overviews
ALTER COLUMN colorizer
SET DATA TYPE "Colorizer"
USING pg_temp.migrate_colorizer(colorizer);

-- drop old types

DROP FUNCTION
pg_temp.migrate_symbology,
pg_temp.migrate_raster_symbology,
pg_temp.migrate_point_symbology,
pg_temp.migrate_line_symbology,
pg_temp.migrate_polygon_symbology,
pg_temp.migrate_text_symbology,
pg_temp.migrate_stroke_param,
pg_temp.migrate_color_param,
pg_temp.migrate_colorizer;

DROP TYPE
"Symbology_old",
"PolygonSymbology_old",
"LineSymbology_old",
"PointSymbology_old",
"TextSymbology_old",
"StrokeParam_old",
"ColorParam_old",
"RasterSymbology_old",
"RasterColorizer_old",
"Colorizer_old",
"ColorizerType_old";
