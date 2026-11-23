-- Existing layers have no cost model until a producer supplies one.
ALTER TABLE layers ADD COLUMN loading_cost jsonb;
