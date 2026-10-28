ALTER TABLE workflows RENAME TO processing_graphs;
ALTER TABLE processing_graphs RENAME COLUMN workflow TO processing_graph;
ALTER INDEX workflows_pkey RENAME TO processing_graphs_pkey;

ALTER TABLE layers RENAME COLUMN workflow_id TO processing_graph_id;
ALTER TABLE layers RENAME CONSTRAINT layers_workflow_id_fkey
TO layers_processing_graph_id_fkey;

ALTER TABLE project_version_layers
RENAME COLUMN workflow_id TO processing_graph_id;
ALTER TABLE project_version_plots
RENAME COLUMN workflow_id TO processing_graph_id;
ALTER TABLE quota_log RENAME COLUMN workflow_id TO processing_graph_id;
