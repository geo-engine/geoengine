# Channel-based STAC request coordinator

## Scope and reuse

This is an alternative to PR #1343, based on its implementation at
`db9fd1c2c`. Reuse origin/authentication identity, STAC client integration,
pagination, grid-cache coalescing, rate-limit header parsing, rolling-window
history, conservative remaining/reset accounting, and retry policy. No REST API
or configuration changes are needed.

## Implementation plan

1. Give each coordinator a channel for owned HTTP attempts, with one oneshot
   reply per attempt. Its event loop exclusively owns FIFO admission, request
   history, learned quota windows, outstanding counts, and cooldowns. Poll
   admitted HTTP operations concurrently.
2. Apply completion feedback and release outstanding bookkeeping before replying
   or admitting further work. Keep JSON decoding and bounded retries in
   `execute`; retries rejoin admission after backoff. Read authorization after
   admission so queued requests use a fresh token.
3. Handle dropped replies as cancellation. Remove queued work without charging
   quota, cancel admitted work, and retain admitted start timestamps. Cancellation
   must wake a loop waiting on an admission timer.
4. Own one `StacRequestRegistry` in `PostgresContext` and clone its handle into
   database handles, execution contexts, and provider clients. Preserve quota
   sharing across queries and sessions. CLI harvesting owns one registry per
   operation; standalone providers can receive an explicitly shared registry.
5. Store coordinator entries in `Arc<DashMap<Identity, Arc<Coordinator>>>`.
   Registry construction simply allocates the map and requires no Tokio runtime.
   At first lookup, create both channels and the coordinator together, then start
   its actor on the current Tokio runtime. This removes all `OnceLock` use in the
   coordinator and the separate registry channel worker/thread.
6. Use the DashMap entry API to make lookup, replacement of closed actors, and
   retirement atomic for each identity. Clone the coordinator handle and release
   the map guard before any await or HTTP operation. Retirement must match the
   exact coordinator instance and confirm registry-only strong ownership under
   the entry guard. An older actor cannot remove its replacement.
7. Keep registry references weak inside coordinators. Preserve history and server
   deadlines while actors remain retained, and release map entries after idle
   expiry. Dropping application/client owners releases the registry without a
   reference cycle. Adapt concurrency, cancellation, retention, and integration
   regressions to eager actor startup and direct registry lookup.

The registry uses DashMap's shard locks for short map operations. Request quota
state remains exclusively owned by each channel actor. No map guard crosses an
await. Neither a global registry nor a `OnceLock` is needed.

## Validation

Run from `geoengine/`:

- `cargo fmt --all -- --check`
- `cargo test -p geoengine-services --lib stac --offline -- --test-threads=1`
- `cargo clippy -p geoengine-services --all-features --all-targets --offline -- -D warnings`
- `git diff --check`

The complete repository backend CI remains the broader integration check. The
new PR should target `main` as an alternative to #1343 and describe the actor
scheduler, context-owned DashMap registry, and validation results.

## Validation results

- All 172 selected STAC tests pass, with three ignored, including local HTTP,
  authentication, retry, grid-cache coalescing, provider/database integration,
  concurrent registry lookup, and lookup/retirement race coverage.
- Strict Clippy for all service features and targets, Rust formatting, Markdown
  formatting, and `git diff --check` pass.
- `CARGO_NET_OFFLINE=true just ci backend` stops during dependency installation
  because `cargo-llvm-cov` is not in the local cache. The complete backend CI
  remains pending.
