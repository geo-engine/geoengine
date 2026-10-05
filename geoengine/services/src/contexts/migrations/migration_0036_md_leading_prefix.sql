-- Store which slice of a 4D array an MD dataset row is.
--
-- `z` is dimension 0 and the last two dimensions are `(y, x)`. Any dimension in between is
-- the *leading prefix*: a fixed index that turns a 4D array into a 3D one, so
-- `(time, depth, y, x)` with `leading_prefix = [2]` is the depth-2 row.
--
-- It is per row, not per dataset, so the bands of one dataset can each select a different
-- slice: one `(time, depth, y, x)` file with one row per depth becomes a single dataset whose
-- band `b` is depth `b`, sharing one time axis. A dataset-level prefix could only express
-- "one dataset = one depth", which then needed one dataset per depth plus a stacker.
--
-- `NOT NULL DEFAULT '{}'` covers every row written before this migration, which is all of
-- them, since 0034 (which created the table) is unreleased. Unlike `ALTER TYPE ... ADD
-- ATTRIBUTE`, `ALTER TABLE` does accept both, so a NULL cannot creep in and fail
-- deserialization into `Vec<i64>` later.
ALTER TABLE dataset_md_tiles
ADD COLUMN leading_prefix bigint[] NOT NULL DEFAULT '{}';
