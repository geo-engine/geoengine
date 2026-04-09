INSERT INTO pro_layer_providers (
    id,
    type_name,
    name,
    definition,
    priority
)
VALUES (
    '409add03-2bfa-43da-86d1-6de18cbd1e50',
    'SentinelS2L2ACogsProviderDefinition',
    'SentinelS2L2ACogsProviderDefinition',
    (
        (
            'Element 84 AWS STAC',
            '409add03-2bfa-43da-86d1-6de18cbd1e50',
            '/v0/collections/sentinel-s2-l2a-cogs/items',
            ARRAY[]::"StacBand" [], -- noqa: PRS
            ARRAY[]::"StacZone" [],
            (1, 100, 2.0)::"StacApiRetries",
            '(999)'::"GdalRetries",
            0,
            'Access to Sentinel 2 L2A COGs on AWS',
            10,
            (1, 10)::"StacQueryBuffer"
        )::"SentinelS2L2ACogsProviderDefinition", -- noqa: PRS
        NULL
    )::"ProDataProviderDefinition",
    10
),
(
    'd3cd1013-c41f-4ac7-938b-3a50e1b9ae5e',
    'CopernicusDataspaceDataProviderDefinition',
    'CopernicusDataspaceDataProviderDefinition',
    (
        NULL, -- noqa: PRS
        (
            'Copernicus Dataspace Data',
            'd3cd1013-c41f-4ac7-938b-3a50e1b9ae5e',
            'https://catalogue.dataspace.copernicus.eu/stac',
            'dataspace.copernicus.eu',
            'XYZ',
            'XYZ',
            'Access to Copernicus Dataspace Data',
            10,
            ARRAY[ARRAY['key', 'VALUE']::"StringPair"]::"StringPair" []
        )::"CopernicusDataspaceDataProviderDefinition"
    )::"ProDataProviderDefinition",
    10
);
