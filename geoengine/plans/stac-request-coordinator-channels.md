# Channel-based STAC request coordinator

## Scope and reuse

This is an alternative to PR #1343, based on its implementation at
`db9fd1c2c`. Keep the existing origin/authentication identity, STAC client
integration, pagination, grid-cache coalescing, rate-limit header parsing,
rolling-window history, conservative remaining/reset accounting, and retry
policy. No API or configuration changes are planned.

## Implementation plan

1. Replace the scheduler mutex, notification queue, and request guards with an
   actor per coordinator. Submit owned HTTP attempts through a Tokio channel;
   each attempt carries a oneshot reply sender. Keep synchronous provider
   construction possible by starting the actor lazily.
2. Give the actor exclusive ownership of FIFO admission, request history, learned
   quota windows, outstanding counts, and cooldowns. Its event loop accepts new
   jobs, processes completed attempts, and wakes at the next admission deadline.
   Poll admitted HTTP attempts concurrently, so a slow response does not stop
   accepting or dispatching other eligible jobs.
3. Apply header feedback and release outstanding bookkeeping before replying.
   Return the response plus the server-controlled retry decision. Retain JSON
   decoding and bounded retries in `execute`; retries join the channel again
   after their existing backoff. Read authorization only after admission.
4. Handle dropped reply receivers as cancellation. Remove queued jobs without
   consuming capacity, cancel admitted HTTP work, and retain the recorded start
   for admitted attempts. Cancellation must wake a loop waiting on a quota
   deadline. Keep coordinator history across provider reconstruction until all
   local history, remote resets, and cooldowns expire.
5. Adapt existing tests to pure scheduler state or channel commands, and exercise
   actual HTTP concurrency, FIFO admission, cancellation, response routing,
   shared quota feedback, retries, authentication, pagination, and cache sharing.

The process-wide registry may retain its short synchronous lookup mutex.
Request admission, completion, cancellation, and retry decisions must not lock
scheduler state. An actor must not retain itself indefinitely or require a
Tokio runtime during synchronous provider construction.

## Validation

Run from `geoengine/`:

- `cargo fmt --all -- --check`
- `cargo test -p geoengine-services --lib stac --offline -- --test-threads=1`
- `cargo clippy -p geoengine-services --all-features --all-targets --offline -- -D warnings`
- `git diff --check`

The complete repository backend CI remains the broader integration check.
The new PR should target `main` as an alternative to #1343 and state which
implementation it reuses, the remaining registry mutex, and validation results.

## Validation results

- Strict Clippy for all service features and targets, Rust formatting, Markdown
  formatting, and `git diff --check` pass.
- All 24 coordinator tests that do not require sockets pass, including the actual
  actor's concurrent admission, FIFO, cancellation, out-of-order replies, and
  provider reconstruction tests. Ten mocked HTTP coordinator tests also compile.
- The full STAC selection has 119 passing tests, 44 failures from denied local
  HTTP listeners or database sockets, and three ignored tests. Every failure
  reports `PermissionDenied`; socket-dependent tests also fail on the unchanged
  implementation in this workspace.
- `CARGO_NET_OFFLINE=true just ci backend` stops during dependency installation
  because `cargo-llvm-cov` is unavailable in the local registry cache. Complete
  backend CI and the HTTP/database tests require a host with network access.
