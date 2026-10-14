INSERT INTO layer_providers (
    id,
    type_name,
    name,
    definition
)
VALUES (
    'ad12176c-29bd-43c2-a3db-05271386da61',
    'Aruna',
    'Aruna test provider',
    ROW(
        ROW(
            'ad12176c-29bd-43c2-a3db-05271386da61'::uuid,
            'Aruna test provider',
            '',
            '',
            '',
            '',
            NULL::int,
            '',
            0::smallint
        )::"ArunaDataProviderDefinition",
        NULL::"GbifDataProviderDefinition",
        NULL::"GfbioAbcdDataProviderDefinition",
        NULL::"GfbioCollectionsDataProviderDefinition",
        NULL::"EbvPortalDataProviderDefinition",
        NULL::"NetCdfCfDataProviderDefinition",
        NULL::"PangaeaDataProviderDefinition",
        NULL::"EdrDataProviderDefinition",
        NULL::"DatasetLayerListingProviderDefinition",
        NULL::"SentinelS2L2ACogsProviderDefinition",
        NULL::"CopernicusDataspaceDataProviderDefinition",
        NULL::"WildliveDataConnectorDefinition",
        NULL::"StacDataProviderDefinition"
    )::"DataProviderDefinition"
);