CREATE TYPE "StacProviderS3Config" AS (
    endpoint text,
    access_key text,
    secret_key text
);

CREATE TYPE "StacProviderDatasetBand" AS ("name" text);

CREATE TYPE "StacProviderDataset" AS (
    "name" text,
    description text,
    data_type "RasterDataType",
    resolution "SpatialResolution",
    projection "SpatialReference",
    spatial_grid "SpatialGridDescriptor",
    bands "StacProviderDatasetBand" []
);

CREATE TYPE "StacDataProviderDefinition" AS (
    "name" text,
    id uuid,
    description text,
    priority smallint,
    api_url text,
    collection_name text,
    s3_config "StacProviderS3Config",
    time_dimension "TimeDimension",
    datasets "StacProviderDataset" []
);

ALTER TYPE "DataProviderDefinition" ADD ATTRIBUTE
stac_data_provider_definition "StacDataProviderDefinition";
