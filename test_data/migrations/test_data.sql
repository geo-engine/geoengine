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
