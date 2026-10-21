# STAC request coordinator implementation plan

## Purpose and scope

All STAC HTTP attempts within one backend process use a FIFO coordinator shared
by configured server origin (scheme, host, effective port) and full configured
authentication identity (token endpoint, client id, username, password), or
anonymous identity.
Collection, provider id, page size, and refreshed access tokens are not identity.
For each server, all anonymous requests share one quota bucket; matching
configured authentication shares another bucket, across all providers and
collections. Different authentications have independent quotas. Pagination follows
the server's next-page URLs using the provider's initialized coordinator and
authentication.
OAuth token endpoints and GDAL asset reads remain outside STAC coordination.

## Scheduling

Add a private `coordinator` module with a process-wide registry, secret-safe
identity, and shared scheduler state. Retain registry entries while a client or
attempt holds them, and for at least the longest learned window after activity;
prune unused expired entries on registry lookup. Retention also covers every
active server reset and cooldown deadline. This preserves quota history
across provider reconstruction. Serialize enqueue/admission with FIFO tickets and Tokio notifications,
but allow HTTP attempts to overlap after reserving quota: a fresh twenty-request
window admits twenty attempts without waiting for any of their responses. Wake
only the front FIFO waiter; only that waiter runs an admission timer. Learn
server policies as response headers arrive, using the fallback rate immediately
instead of serializing a bootstrap request. Reserve quota before dispatch;
release outstanding bookkeeping even on cancellation or
network failure. Do not hold cache locks during queue waits or HTTP calls.

Track all advertised quota windows using one shared rolling timestamp history. Parse
`x-ratelimit-limit: 10, 10;w=1, 60;w=60`, `remaining: 9`, and `reset: 1`.
Reset is a relative duration in seconds. Locally enforce both windows; observed
remaining/reset additionally accounts for other clients. Concurrent responses
must not replenish quota using stale observations or omit outstanding requests.
Policies may appear as repeated header triples or alternate between responses as
the binding policy changes. Deduplicate windows by duration and retain policies
that are absent from an individual response. Track remaining/reset independently
for each identifiable policy. Every known window must permit admission, so the
latest blocking deadline determines the next dispatch. Use monotonic deadlines.
Missing or malformed headers must not panic or stall forever. Avoid uncontrolled
bursts when quota information is missing.

A 429 updates a shared cooldown, honoring Retry-After (seconds and HTTP date)
and reset, with a one-second fixed fallback when neither is usable. Missing quota headers
use a conservative fallback of ten requests per second from the first request. Retry the rejected
attempt through FIFO admission; bounded retries retain the existing failure
contract. Do not feed exhausted 429 retries into generic exponential retry.
Keep existing retry behavior for other transient HTTP, transport and JSON errors.
Build authorization at dispatch time so time spent queued cannot stale the token.

## Integration with the grid cache

Attach the coordinator to the STAC client during provider construction and auth
initialization. Route first and subsequent pages through the coordinated send
API. Preserve provider-specific transport timeout settings. The coordinator owns
HTTP dispatch, header observation and both rate-limit and transient retries.
Callers receive decoded JSON or an error; permits and queue admission
are private implementation details. JSON decode failures retain their existing
retry policy inside the coordinator, alongside HTTP and transport failures.

Moka `StacQueryCache::get_or_fetch` remains the owner of dataset/cell/time-step
coalescing. Its initializer covers queue waits, cooldowns, all pages, and result
assembly. Matching callers attach to the same initializer even while it is queued
or rate-limited. Complete results alone are admitted; failures stay retryable.
No cross-collection result sharing or changes to grid geometry are needed.

## Verification and review

Test identity sharing and separation, origin normalization and credential
redaction, FIFO dispatch, short and long windows, header parsing, remaining/reset,
out-of-order response feedback, shared 429 cooldown/recovery, missing headers,
queued/in-flight cancellation, and authorization after waits. Add a mocked HTTP
integration regression combining independent providers, queue pressure,
pagination and same-grid-cell coalescing (including disabled result retention).
Run formatting, the STAC service tests, and applicable clippy checks. Review
throughput, retry bounds, deadlocks, secret handling, cancellation and stale
header races; correct findings and rerun affected checks.

## Implementation details

Short synchronous mutex sections protect scheduler state; no guard spans an
await. Cancellation removes queued waiters or releases in-flight bookkeeping
synchronously, including during runtime shutdown. One guard follows each attempt
through `Queued`, `InFlight`, and `Finished`; admission results are a separate
enum. There is no discovery phase. Request counts represent actual counts.

Normal completion (including transport errors) explicitly releases the attempt
and applies response feedback with one scheduler lock. The guard's `Drop` only
locks for cancellation/unwinding. Lock sections contain no awaits, and recover
poisoned mutexes so cleanup cannot panic again because a mutex was poisoned.
Enqueueing and the first admission check also share a single lock acquisition.
The registry mutex is independent and is used only for lookup/reclamation;
only lookup acquires both locks, in registry-to-state order.

All server feedback can lower capacity, irrespective of dispatch order. It cannot
refill capacity until reset, because dispatch order need not match server
processing order. Authorization and transport timeouts are applied at dispatch.
Existing grid geometry and Moka cache matching stay unchanged.

Only the one-second fallback delay and five-minute minimum idle retention are
fixed constants. Advertised request counts, window durations and cooldowns are
not capped. Unrepresentable deadlines are ignored. Every window uses the same
request history, retained for the longest known window or five minutes, whichever
is longer. The limit-th most recent timestamp determines local capacity in O(1)
per policy. Header policies and feedback are parsed together once, outside the
scheduler lock, using the same `Window` type as scheduler state. Normal request
processing never locks the process-wide registry.

## Validation commands

- `cargo test -p geoengine-services --lib stac -- --test-threads=1` covers the
  provider, coordinator, grid cache, authentication, harvester and discovery.
  Serial test execution avoids interference between tests that install logging
  subscribers; individual scheduling and HTTP regressions still exercise
  concurrent requests explicitly.
- `cargo clippy -p geoengine-services --all-features --all-targets -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
