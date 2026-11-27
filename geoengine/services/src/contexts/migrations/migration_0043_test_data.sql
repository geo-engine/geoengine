INSERT INTO processing_graphs (id, processing_graph) VALUES
(
    '00000000-0000-0000-0000-000000000001',
    '{
        "type": "Raster",
        "operator": {"type": "GdalSource", "params": {"data": "ndvi"}}
    }'
),
(
    '00000000-0000-0000-0000-000000000002',
    '{
        "type": "Vector",
        "operator": {"type": "OgrSource", "params": {"data": "points"}}
    }'
);

INSERT INTO projects (id) VALUES ('00000000-0000-0000-0000-0000000000a1');

INSERT INTO project_versions (
    id, project_id, name, description, bounds, time_step, changed
) VALUES
(
    '00000000-0000-0000-0000-0000000000b1',
    '00000000-0000-0000-0000-0000000000a1',
    'Project',
    'Version 1',
    ROW(
        ROW('Epsg', 4326)::"SpatialReference",
        ROW(
            ROW(0, 0)::"Coordinate2D", ROW(1, 1)::"Coordinate2D"
        )::"BoundingBox2D",
        ROW(0, 1)::"TimeInterval"
    )::"STRectangle",
    ROW('Days', 1)::"TimeStep",
    '2026-01-01T00:00:00Z'
),
(
    '00000000-0000-0000-0000-0000000000b2',
    '00000000-0000-0000-0000-0000000000a1',
    'Project',
    'Version 2',
    ROW(
        ROW('Epsg', 4326)::"SpatialReference",
        ROW(
            ROW(0, 0)::"Coordinate2D", ROW(1, 1)::"Coordinate2D"
        )::"BoundingBox2D",
        ROW(0, 1)::"TimeInterval"
    )::"STRectangle",
    ROW('Days', 1)::"TimeStep",
    '2026-01-02T00:00:00Z'
);

INSERT INTO project_version_layers (
    layer_index,
    project_id,
    project_version_id,
    name,
    processing_graph_id,
    symbology,
    visibility
) VALUES
-- version 1: raster, vector
(
    0,
    '00000000-0000-0000-0000-0000000000a1',
    '00000000-0000-0000-0000-0000000000b1',
    'Raster',
    '00000000-0000-0000-0000-000000000001',
    NULL,
    ROW(TRUE, FALSE)::"LayerVisibility"
),
(
    1,
    '00000000-0000-0000-0000-0000000000a1',
    '00000000-0000-0000-0000-0000000000b1',
    'Vector',
    '00000000-0000-0000-0000-000000000002',
    NULL,
    ROW(TRUE, FALSE)::"LayerVisibility"
),
-- version 2: raster renamed, vector replaced by a layer without a graph
(
    0,
    '00000000-0000-0000-0000-0000000000a1',
    '00000000-0000-0000-0000-0000000000b2',
    'Raster renamed',
    '00000000-0000-0000-0000-000000000001',
    NULL,
    ROW(FALSE, TRUE)::"LayerVisibility"
),
(
    1,
    '00000000-0000-0000-0000-0000000000a1',
    '00000000-0000-0000-0000-0000000000b2',
    'Orphan',
    '00000000-0000-0000-0000-0000000000ff',
    NULL,
    ROW(TRUE, FALSE)::"LayerVisibility"
);

INSERT INTO project_version_plots (
    plot_index,
    project_id,
    project_version_id,
    name,
    processing_graph_id
) VALUES
(
    0,
    '00000000-0000-0000-0000-0000000000a1',
    '00000000-0000-0000-0000-0000000000b1',
    'Plot',
    '00000000-0000-0000-0000-000000000001'
),
(
    0,
    '00000000-0000-0000-0000-0000000000a1',
    '00000000-0000-0000-0000-0000000000b2',
    'Plot',
    '00000000-0000-0000-0000-000000000001'
);
