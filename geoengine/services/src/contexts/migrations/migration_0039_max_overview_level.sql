-- The power-of-two overview levels 2 .. max_overview_level that every file of
-- a source dataset has. NULL means the levels are unknown: the source then lets
-- GDAL choose or resample, as before. 0 or 1 means the dataset has no
-- overviews.
ALTER TYPE "SpatialGridDescriptor" ADD ATTRIBUTE max_overview_level integer;
