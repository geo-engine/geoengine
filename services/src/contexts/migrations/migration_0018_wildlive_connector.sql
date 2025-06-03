CREATE TYPE "WildliveDataConnectorDefinition" AS (
    id uuid,
    "name" text,
    description text,
    api_key text,
    priority smallint
);

ALTER TYPE "DataProviderDefinition" ADD ATTRIBUTE
wildlive_data_connector_definition "WildliveDataConnectorDefinition";

CREATE TABLE wildlive_projects (
    provider_id uuid NOT NULL,
    cache_date date NOT NULL,
    project_id text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    geom public.GEOMETRY (POLYGON) NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, cache_date, project_id) DEFERRABLE
);

CREATE TABLE wildlive_stations (
    provider_id uuid NOT NULL,
    cache_date date NOT NULL,
    station_id text NOT NULL,
    project_id text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    location text NOT NULL,
    geom public.GEOMETRY (POINT) NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, cache_date, project_id, station_id) DEFERRABLE
);

CREATE TABLE wildlive_captures (
    provider_id uuid NOT NULL,
    cache_date date NOT NULL,
    image_object_id text NOT NULL,
    project_id text NOT NULL,
    station_setup_id text NOT NULL,
    capture_time_stamp timestamp with time zone NOT NULL,
    accepted_name_usage_id text NOT NULL,
    vernacular_name text NOT NULL,
    scientific_name text NOT NULL,
    content_url text NOT NULL,
    geom public.GEOMETRY (POINT) NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (
        provider_id, cache_date, project_id, image_object_id
    ) DEFERRABLE
);
