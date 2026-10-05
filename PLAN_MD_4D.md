# 4D support for `MdGdalSource`: a leading-index prefix

## The idea

A 4D array is read one z-slice at a time, with the remaining leading dimensions held at a
fixed **prefix**. That is not a new mechanism — the read path already has it:

```rust
// process_impl.rs:775-783 — this is the prefix mechanism already
let mut array_start_index = vec![0u64; num_dimensions];   // leading dims: start 0
array_start_index[num_dimensions - 3] = z_range.start;      // only the z dim moves
let mut array_count = vec![1usize; num_dimensions];          // leading dims: count 1
array_count[num_dimensions - 3] = z_range.len();
md_array.read_as::<T>(array_start_index, array_count)?;    // per-dimension window
```

GDAL already takes a per-dimension start + count, so "read time slices of a 4D array with the
other leading dimension fixed" is a one-line change. The only thing preventing it is the
probe's rejection of non-singleton leading dimensions:

```rust
// probe.rs:205-213
if leading.iter().any(|d| d.size() != 1) {
    return Err(... "non-singleton leading dimensions (unsupported layout)" ...);
}
```

### Relation to the netCDF hierarchy

Analogous, but complementary rather than identical. The group path is *named* selection
(which array); the prefix is *positional* selection (which slice of the leading dimensions).
A full address needs both.

| | selects | netcdfcf | this design |
|---|---|---|---|
| group path `/metric_1/` | which array | yes | yes (unchanged) |
| leading-index prefix | which leading slice | **no** — flattened into band indices | yes |

netcdfcf has hierarchy but no prefix: it flattens the leading dimensions into band indices
(`entity * n_time + i + 1`), which assumes how GDAL's netCDF classic view flattens leading
dims. That assumption is exactly what breaks on NEX-GDDP-CMIP6. This design has both, and
deletes the arithmetic.

## Scope: z is the outermost dimension

```
(time, depth, y, x)   -> z = time, prefix = [depth]     supported
(depth, time, y, x)   -> z = depth, prefix = [time]     not supported
(entity, time, y, x)  -> EBV order                       not supported
```

z is restricted to dimension 0, deliberately:

- For a 3D array `num_dimensions - 3 == 0`, so this is **additive** — existing datasets read
  identically and no regression is possible.
- z elsewhere would need a stored `z_dim_index`, strided reads, and re-derivation from every
  file's dimension list at read time.

The EBV order is excluded, and that is fine: `netcdfcf` is not being replaced (see
`PLAN_MD_GDAT_SOURCE.md`, review round 4), so EBV keeps its provider. This plan is for
`(time, depth, y, x)`-style data. **This is a deliberate scope cut, not an oversight.**

## The prefix is a dataset-level property

A different prefix is a different sub-array, so a different dataset. It therefore belongs
beside `z_role` / `wrap` / `cache_ttl` in `GdalMdMetaData`, **not** on `dataset_md_tiles`:

- A row already means "(file, variable), all of its z slices", which is unchanged.
- No schema change to the per-file table, so gap filling, `z_start`/`z_end` realignment,
  `local_offset` clipping and `z_batches` all keep working untouched.

The cost: one depth per dataset. Composing several depths is `RasterStacker`'s job.

## Changes

### 1. Read path — `process_impl.rs:775-783`

```rust
// before
array_start_index[num_dimensions - 3] = z_range.start as u64;
// after
array_start_index[0] = z_range.start as u64;   // z is the outermost dim
for (i, v) in leading_prefix.iter().enumerate() {
    array_start_index[1 + i] = *v as u64;
}
```

`array_count` is unchanged: prefix dims keep count 1.

### 2. IPC — `GdalReadKind::MdArray`

The worker currently infers every dimension position from `num_dimensions` alone, so the
prefix cannot reach it. Add `leading_prefix: Vec<u64>` to the variant.

### 3. Probe — `probe.rs:205-218`

- Drop the size-1 rejection.
- `z_dim = &dimensions[0]`; `y`/`x` stay the last two.
- Validate the requested prefix against each leading dimension's size.
- Report the discovered leading dimensions (name + size) so a client can discover them.

### 4. Dataset metadata and migration

`GdalMdMetaData.leading_prefix bigint[]` (default `{}`), mirroring how `cache_ttl` was added.
Migration `0035_md_leading_prefix.sql`, one `ALTER TYPE`.

### 5. Service validation — `datasets.rs:803`

`dimensions[len-3].size()` becomes `dimensions[0].size()`, and the declared prefix is checked
against the file's leading dimensions.

### 6. Regenerate

OpenAPI plus all three API clients — `leadingPrefix` lands on `GdalMdMetaData` and
`MdProbeRequest`.

## Gate

New fixture `time_depth_4d.nc`, `(time, depth, y, x)`:

- probe reports the leading dimensions and accepts a prefix
- reading with `prefix = [2]` yields exactly depth 2's time series
- an out-of-range prefix errors at the trust boundary
- a 3D fixture reads identically to before (no regression)

## Ceiling

The prefix is fixed per dataset, so **depth-as-bands is not expressible** — one depth per
dataset, composed with `RasterStacker`. The alternative (a row-level prefix plus a band
mapping) is rejected deliberately: it reintroduces the flattening ambiguity that made
`netcdfcf`'s `entity * n_time + i + 1` fragile, and would put the assumption back into the one
place this work exists to remove it.

The upgrade path, if depth-as-bands is ever needed, is a per-row prefix plus an explicit
`band -> (prefix index)` mapping so the mapping is *declared* rather than *computed*.
