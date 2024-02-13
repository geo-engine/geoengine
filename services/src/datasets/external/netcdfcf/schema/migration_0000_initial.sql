CREATE TABLE geoengine (
    database_version text NOT NULL
);

INSERT INTO geoengine (database_version) VALUES (
    '0000_initial'
);

CREATE TYPE "NetCdfEntity" (
    id bigint NOT NULL,
    name text NOT NULL
);

CREATE TABLE overviews (
    file_name text NOT NULL,
    title text NOT NULL,
    summary text NOT NULL,
    -- spatial_reference "SpatialReference" NOT NULL,
    -- groups NetCdfGroup [] NOT NULL,
    -- entities "NetCdfEntity" [] NOT NULL,
    -- time_coverage "TimeCoverage" NOT NULL,
    -- colorizer "Colorizer" NOT NULL,
    creator_name text,
    creator_email text,
    creator_institution text
);
