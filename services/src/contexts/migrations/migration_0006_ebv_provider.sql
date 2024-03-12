ALTER TYPE "EbvPortalDataProviderDefinition" RENAME ATTRIBUTE "path" TO "data";
ALTER TYPE "NetCdfCfDataProviderDefinition" RENAME ATTRIBUTE "path" TO "data";

-- EBV PROVIDER TABLE DEFINITIONS

CREATE TABLE ebv_provider_locks (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name)
);

CREATE TABLE ebv_provider_overviews (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    title text NOT NULL,
    summary text NOT NULL,
    spatial_reference "SpatialReference" NOT NULL,
    colorizer "Colorizer" NOT NULL,
    creator_name text,
    creator_email text,
    creator_institution text,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name)
);

CREATE TABLE ebv_provider_groups (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    name text [] NOT NULL,
    title text NOT NULL,
    description text NOT NULL,
    data_type "RasterDataType",
    data_range float [2],
    unit text NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name, name) DEFERRABLE,

    FOREIGN KEY (provider_id, file_name) REFERENCES ebv_provider_overviews (
        provider_id,
        file_name
    ) ON DELETE CASCADE DEFERRABLE
);

CREATE TABLE ebv_provider_entities (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    id bigint NOT NULL,
    name text NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name, id) DEFERRABLE,

    FOREIGN KEY (provider_id, file_name) REFERENCES ebv_provider_overviews (
        provider_id,
        file_name
    ) ON DELETE CASCADE DEFERRABLE
);

CREATE TABLE ebv_provider_timestamps (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    "time" bigint NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name, "time") DEFERRABLE,

    FOREIGN KEY (provider_id, file_name) REFERENCES ebv_provider_overviews (
        provider_id,
        file_name
    ) ON DELETE CASCADE DEFERRABLE
);

CREATE TABLE ebv_provider_loading_infos (
    provider_id uuid NOT NULL,
    file_name text NOT NULL,
    group_names text [] NOT NULL,
    entity_id bigint NOT NULL,
    meta_data "GdalMetaDataList" NOT NULL,

    -- TODO: check if we need it
    PRIMARY KEY (provider_id, file_name, group_names, entity_id) DEFERRABLE,

    FOREIGN KEY (provider_id, file_name) REFERENCES ebv_provider_overviews (
        provider_id,
        file_name
    ) ON DELETE CASCADE DEFERRABLE
);
