---
name: new-migration
description: Add a new PostgreSQL schema or data migration to the Geo Engine backend (geoengine/services/src/contexts/migrations), including registration, current_schema.sql, Rust DB type mappings and tests. Use when a change needs new or altered tables, columns, composite types, enums or a data backfill.
---

# New database migration

Migrations live in `geoengine/services/src/contexts/migrations/` (paths below are relative to it unless they start with `geoengine/`).
They run automatically on server start inside a single transaction; there is no manual migration step.

Use the user's migration name, or derive one from the task or changes and state it. Ask for a short description only if neither works.

## 1. Name and number

- Name: `snake_case`, short, describing the change (e.g. `stac_provider_cache_ttl`).
- Number: highest existing `migration_NNNN_*` file plus one, zero-padded to 4 digits (enforced by the `versions_follow_schema` test).
- Files: `migration_NNNN_<name>.rs` and, for SQL, `migration_NNNN_<name>.sql`. Struct: `MigrationNNNN<CamelCaseName>`. Version string: `"NNNN_<name>"`.
- `prev_version` is the last entry of `all_migrations()` in `mod.rs`.

## 2. Write the migration

Pick the shape that fits:

- **Pure DDL/SQL**: `.rs` + `.sql`, the `.rs` runs `tx.batch_execute(include_str!("migration_NNNN_<name>.sql"))`. Template: `migration_0033_gdal_multiband_cache_ttl.rs`.
- **Needs Rust values** (role ids, permissions, computed data): logic in the `.rs` with `tx.execute`/`tx.prepare`. Example: `migration_0021_default_permissions_for_existing_providers.rs`.

Put a `///` doc comment on the struct that says what the migration changes and why.

Decide what happens to **existing rows**: defaults, backfill, or `NULL` with documented meaning (see the comments in `migration_0033_gdal_multiband_cache_ttl.sql`).

PostgreSQL pitfalls:

- `ALTER TABLE … ADD COLUMN` and `ALTER TYPE … ADD ATTRIBUTE` append at the end.
- `ALTER TYPE … ADD VALUE` (enum): the new value cannot be used later in the same transaction, and the whole migration is one transaction.

## 3. Register it in `mod.rs`

Three places: the `mod migration_NNNN_<name>;` list, the `pub use` block, and append `Box::new(MigrationNNNN…)` to the end of `all_migrations()`.
`CurrentSchemaMigration` takes its version from the last entry, so nothing else needs updating there.

## 4. Update `current_schema.sql`

Fresh databases are created from this file directly, so it must describe the **end state** with plain declarations, not `ALTER` statements.
The test `migrations_lead_to_ground_truth_schema` compares it with the result of running all migrations:

- Table columns are compared by `ordinal_position`: put a new column **last**, matching where `ADD COLUMN` puts it.
- Composite type attributes are compared by name; still append them for readability.
- Constraints, enums, domains and views are compared too.

## 5. Update Rust mapping types

If a table or composite type changes, update the Rust type mapped to it.
Grep for the type name, e.g. `geoengine/services/src/contexts/db_types.rs` or `geoengine/operators/src/source/gdal_source/db_types.rs`.

- **Derive** `ToSql`/`FromSql` (with `#[postgres(name = "…")]`) directly on the Rust type if it maps 1:1: a struct whose fields match the composite type, or a fieldless enum that maps to a PostgreSQL enum (e.g. `FeatureDataType`).
- **Delegate** if it does not: enums with data (PostgreSQL has no sum types), or types whose shape differs or that need validation on read.
  Add a `…DbType` struct that derives the traits, usually with a discriminator enum and `Option<…>` fields per variant, implement `From<&T> for TDbType` and `TryFrom<TDbType> for T`, and call `delegate_from_to_sql!(T, TDbType)`. Example: `ColorParamDbType` in `geoengine/services/src/contexts/db_types.rs`.

Field names must match the column or attribute names. New nullable fields are `Option<…>`.
Then fix the code that builds or reads these types (`From`/`TryFrom` impls, inserts and queries).

## 6. Tests

If the migration transforms existing data, add a `#[cfg(test)] mod tests` in the `.rs` following `migration_0023_wildlive_oidc.rs`:

1. `create_migration_0015_snapshot(&mut conn)`
2. `migrate_database(&mut conn, &migrations_by_range(&Migration0016MergeProviders.version(), &<prev>.version()))`
3. Insert fixtures from `migration_NNNN_test_data.sql`
4. Run this migration and assert the resulting data.

Pure additive DDL (e.g. a new nullable column) is covered by the schema comparison test.

## 7. SQL style

PostgreSQL, linted by sqlfluff (`geoengine/.sqlfluff`): keywords uppercase, table and column names lowercase.

## 8. Verify

Tests need a local Postgres user/database `geoengine`/`geoengine` with PostGIS (see `geoengine/README.md`).

- `just backend test-services contexts::migrations`
- `just backend lint-sql`
- `just backend lint-clippy`
- If types used by the API changed: `just backend test-services` for the affected module.

If Postgres is not available, say so and do not claim the migration was verified.

## Don'ts

- Never edit, renumber or delete a migration that is already on `main`. Fix mistakes with a new migration.
- Never hand-edit `migration_0015_snapshot.sql`.
