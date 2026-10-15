-- `Statistics` and `BoxPlot` take a single (multi-band) raster
-- instead of a list of rasters.
-- Plot workflows with exactly one raster source get this raster as source.
-- Their `columnNames` were aliases for the rasters and now select bands
-- by name, so they are cleared.
-- Workflows with several raster sources cannot be expressed anymore
-- and are left unchanged.
UPDATE workflows
SET
    workflow = jsonb_set(
        jsonb_set(
            workflow::jsonb,
            '{operator,sources,source}',
            workflow::jsonb #> '{operator,sources,source,0}'
        ),
        '{operator,params,columnNames}',
        '[]'::jsonb
    )::json
WHERE
    workflow ->> 'type' = 'Plot'
    AND workflow #>> '{operator,type}' IN ('Statistics', 'BoxPlot')
    AND json_typeof(workflow #> '{operator,sources,source}') = 'array'
    AND json_array_length(workflow #> '{operator,sources,source}') = 1;
