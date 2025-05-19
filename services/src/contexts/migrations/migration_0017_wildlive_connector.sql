CREATE TYPE "WildliveDataConnectorDefinition" AS (
    id uuid,
    "name" text,
    description text,
    api_key text,
    priority smallint
);

ALTER TYPE "DataProviderDefinition" ADD ATTRIBUTE
wildlive_data_connector_definition "WildliveDataConnectorDefinition";
