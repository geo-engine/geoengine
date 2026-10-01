-- NULL inherits the configured global default
-- a tile-level TTL still takes precedence.
-- A stored zero explicitly disables caching at the dataset level.
ALTER TYPE "GdalMultiBand" ADD ATTRIBUTE cache_ttl int;
