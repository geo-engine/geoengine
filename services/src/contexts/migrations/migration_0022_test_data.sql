INSERT INTO layer_providers (
    id,
    type_name,
    name,
    definition
)
VALUES (
    'ad12176c-29bd-43c2-a3db-05271386da61',
    'Wildlive',
    'Wildlive 1',
    (
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        (
            'ad12176c-29bd-43c2-a3db-05271386da61',
            'Wildlive 1',
            'description',
            'an api key',
            0
        )::"WildliveDataConnectorDefinition"
    )::"DataProviderDefinition"
), (
    '92a6e057-f345-4162-91b1-1545c8f793b9',
    'Wildlive',
    'Wildlive 2',
    (
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        (
            '92a6e057-f345-4162-91b1-1545c8f793b9',
            'Wildlive 2',
            'description',
            NULL,
            0
        )::"WildliveDataConnectorDefinition"
    )::"DataProviderDefinition"
);
