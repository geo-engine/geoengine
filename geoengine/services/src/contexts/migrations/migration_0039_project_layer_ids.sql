-- Project layers and plots become layers: they get an id that is stable across
-- project versions, a description, properties, metadata and an optional
-- reference to the catalog layer they were copied from.

-- Rows whose processing graph does not exist cannot be loaded or rendered.
-- Remove them so that the foreign key below can be added.
DELETE FROM project_version_layers AS l
WHERE NOT EXISTS (
    SELECT 1 FROM processing_graphs AS g
    WHERE g.id = l.processing_graph_id
);

DELETE FROM project_version_plots AS p
WHERE NOT EXISTS (
    SELECT 1 FROM processing_graphs AS g
    WHERE g.id = p.processing_graph_id
);

ALTER TABLE project_version_layers
ADD COLUMN layer_id uuid,
ADD COLUMN description text NOT NULL DEFAULT '',
ADD COLUMN properties "PropertyType"[] NOT NULL
DEFAULT ARRAY[]::"PropertyType"[],
ADD COLUMN metadata "TextTextKeyValue"[] NOT NULL
DEFAULT ARRAY[]::"TextTextKeyValue"[],
ADD COLUMN source_provider_id uuid,
ADD COLUMN source_layer_id text,
ADD FOREIGN KEY (processing_graph_id) REFERENCES processing_graphs (id);

ALTER TABLE project_version_plots
ADD COLUMN layer_id uuid,
ADD COLUMN description text NOT NULL DEFAULT '',
ADD COLUMN properties "PropertyType"[] NOT NULL
DEFAULT ARRAY[]::"PropertyType"[],
ADD COLUMN metadata "TextTextKeyValue"[] NOT NULL
DEFAULT ARRAY[]::"TextTextKeyValue"[],
ADD COLUMN source_provider_id uuid,
ADD COLUMN source_layer_id text,
ADD COLUMN visibility "LayerVisibility" NOT NULL
DEFAULT ROW(TRUE, FALSE)::"LayerVisibility",
ADD FOREIGN KEY (processing_graph_id) REFERENCES processing_graphs (id);

-- A layer at the same position with the same processing graph is the same layer
-- in all versions of a project, so it gets the same id.
WITH ids AS (
    SELECT
        project_id,
        layer_index,
        processing_graph_id,
        GEN_RANDOM_UUID() AS layer_id
    FROM project_version_layers
    GROUP BY project_id, layer_index, processing_graph_id
)

UPDATE project_version_layers AS l
SET layer_id = ids.layer_id
FROM ids
WHERE
    l.project_id = ids.project_id
    AND l.layer_index = ids.layer_index
    AND l.processing_graph_id = ids.processing_graph_id;

WITH ids AS (
    SELECT
        project_id,
        plot_index,
        processing_graph_id,
        GEN_RANDOM_UUID() AS layer_id
    FROM project_version_plots
    GROUP BY project_id, plot_index, processing_graph_id
)

UPDATE project_version_plots AS p
SET layer_id = ids.layer_id
FROM ids
WHERE
    p.project_id = ids.project_id
    AND p.plot_index = ids.plot_index
    AND p.processing_graph_id = ids.processing_graph_id;

ALTER TABLE project_version_layers
ALTER COLUMN layer_id SET NOT NULL,
ALTER COLUMN description DROP DEFAULT,
ALTER COLUMN properties DROP DEFAULT,
ALTER COLUMN metadata DROP DEFAULT;

ALTER TABLE project_version_plots
ALTER COLUMN layer_id SET NOT NULL,
ALTER COLUMN description DROP DEFAULT,
ALTER COLUMN properties DROP DEFAULT,
ALTER COLUMN metadata DROP DEFAULT,
ALTER COLUMN visibility DROP DEFAULT;

CREATE INDEX project_version_layers_layer_id_idx -- noqa: PG01
ON project_version_layers (layer_id);

CREATE INDEX project_version_plots_layer_id_idx -- noqa: PG01
ON project_version_plots (layer_id);
