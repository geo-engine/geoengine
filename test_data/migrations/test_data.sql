-- Test data for initial database schema which will be subjected to migrations 
-- and verified to be loadable in the latest database version.
INSERT INTO projects (id) VALUES ('6a272e75-7ea2-43d7-804d-d84308e0f0fe');

INSERT INTO project_versions (
    id, project_id, name, description, bounds, time_step, changed
) VALUES (
    '516db5fa-854e-493f-b17d-bc5379d712bc',
    '6a272e75-7ea2-43d7-804d-d84308e0f0fe',
    'Test Project',
    'Test Project Description',
    (
        ('Epsg', 4326), -- noqa: PRS
        ((-180, -90), (180, 90)), 
        (0, 10000)
    ),
    ('Hours', 1),
    TIMESTAMP '2014-01-01 00:00:00'
);

INSERT INTO workflows (id, workflow) VALUES (
    '38ddfc17-016e-4910-8adf-b1af36a8590c',
    '{
    "type": "Raster",
    "operator": {
        "type": "GdalSource",
        "params": {
            "data": "ndvi"
        }
    }
}'
);

INSERT INTO layers (
    id,
    name,
    description,
    workflow_id,
    symbology,
    properties,
    metadata
) VALUES (
    '78aaa2b2-7d6b-4e6a-86bf-b3cd1b63553a',
    'Test Layer',
    'Test Layer Description',
    '38ddfc17-016e-4910-8adf-b1af36a8590c',
    (
        (
            1.0, -- noqa: PRS
            (
                'Rgba'::"ColorizerType", -- noqa: PRS
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            )::"Colorizer"
        )::"RasterSymbology", -- noqa: PRS
        NULL,
        NULL,
        NULL
    )::"Symbology",
    ARRAY[]::"PropertyType" [],
    ARRAY[]::"TextTextKeyValue" []
);

INSERT INTO datasets (
    id,
    name,
    display_name,
    description,
    source_operator,
    result_descriptor,
    meta_data,
    symbology
) VALUES (
    '6cc80129-eea4-4140-b09c-6bcfbd76ad5f',
    (NULL, 'test')::"DatasetName",
    'Test Dataset',
    'Test Dataset Description',
    'GdalSource',
    (
        (
            'U8'::"RasterDataType", -- noqa: PRS
            ('Epsg'::"SpatialReferenceAuthority", 4326)::"SpatialReference",
            (NULL, NULL)::"Measurement",
            (0, 0)::"TimeInterval",
            (
                (-180.0, -90.0)::"Coordinate2D",
                (180.0, 90.0)::"Coordinate2D"
            )::"SpatialPartition2D",
            (0.1, 0.1)::"SpatialResolution"
        )::"RasterResultDescriptor", -- noqa: PRS
        NULL, 
        NULL
    )::"ResultDescriptor",
    (
        NULL,
        NULL,
        NULL, -- noqa: PRS
        (
            (0, 0)::"TimeInterval",
             (
                'foo/bar.tiff',
                0,
                (
                    (0.0, 0.0)::"Coordinate2D",
                    0.1,
                    0.1
                )::"GdalDatasetGeoTransform",
                3600,
                1800,
                'Error'::"FileNotFoundHandling",
                0.0,
                array[]::"GdalMetadataMapping"[],
                array[]::text[],
                array[]::"StringPair"[],
                false,
                ROW(0)::"GdalRetryOptions"
            )::"GdalDatasetParameters",
            (
                'U8'::"RasterDataType",
                ('Epsg'::"SpatialReferenceAuthority", 4326)::"SpatialReference",
                (NULL, NULL)::"Measurement",
                (0, 0)::"TimeInterval",
                (
                    (-180.0, -90.0)::"Coordinate2D",
                    (180.0, 90.0)::"Coordinate2D"
                )::"SpatialPartition2D",
                (0.1, 0.1)::"SpatialResolution"
            )::"RasterResultDescriptor",
            0   
        )::"GdalMetaDataStatic",        
        NULL,
        NULL
    )::"MetaDataDefinition",
    (
        (
            1.0, -- noqa: PRS
            (
                'LinearGradient'::"ColorizerType", -- noqa: PRS
                array[(
                        0.0, 
                        array[128,128,128,255]::"RgbaColor"
                    )::"Breakpoint"
                ]::"Breakpoint"[],
                array[0,0,0,0]::"RgbaColor",
                array[0,0,0,0]::"RgbaColor",
                array[0,0,0,0]::"RgbaColor",
                NULL
            )::"Colorizer"
        )::"RasterSymbology", -- noqa: PRS
        NULL,
        NULL,
        NULL
    )::"Symbology"
);

INSERT INTO layer_providers (
    id,
    type_name,
    name,
    definition
) VALUES (
    '1c01dbb9-e3ab-f9a2-06f5-228ba4b6bf7a',
    'GBIF',
    'GBIF',
    (
        NULL, -- noqa: PRS
        (
            'GBIF',
            (
                'localhost',
                5432,
                'geoengine',
                'gbif',
                'geoengine',
                'geoengine'
            )::"DatabaseConnectionConfig",
            0
        )::"GbifDataProviderDefinition",
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    )::"DataProviderDefinition"
);
