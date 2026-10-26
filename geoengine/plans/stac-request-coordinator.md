# STAC request coordinator implementation plan

## Purpose and scope

All STAC HTTP attempts within one application context use a FIFO coordinator shared
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

Add a private `coordinator` module with an application-owned registry, secret-safe
identity, and shared scheduler state. Retain registry entries while a client or
attempt holds them, and for at least the longest learned window after activity;
reclaim unused entries after their actor's retained deadlines expire. Retention
also covers every active server reset and cooldown deadline. This preserves quota
history across provider reconstruction. Each coordinator receives owned HTTP
attempts through a channel and exclusively owns its FIFO queue and quota state.
Its event loop runs admitted attempts concurrently: a fresh twenty-request window
admits twenty attempts without waiting for any of their responses. One timer
tracks the next admission deadline. Learn server policies as response headers
arrive, using the fallback rate immediately instead of serializing a bootstrap
request. Reserve quota before dispatch; release outstanding bookkeeping even on
cancellation or network failure. Send results through each attempt's reply
channel after applying feedback. Do not hold cache locks during queue waits or
HTTP calls.

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
and reset, with a one-second fixed fallback when neither is usable. If no quota
window has been learned and no usable server retry timing is supplied, the retry
loop also applies its existing exponential backoff to 429s. Missing quota headers
use a conservative fallback of ten requests per second from the first request.
Retry the rejected attempt through FIFO admission; bounded retries retain the
existing failure contract. Do not feed exhausted 429 retries into an outer retry loop.
Keep existing retry behavior for other transient HTTP, transport and JSON errors.
Build authorization at dispatch time so time spent queued cannot stale the token.

## Integration with the grid cache

Merge request execution into `StacClient`, which owns its provider-specific HTTP
transport, authentication, bounded retries, and JSON decoding. Route first and
subsequent pages through `execute_json`. Look up a shared `ActorChannels` handle
for the configured origin and authentication identity; the registry holds only
these channel and lifecycle records. The actor owns admission, HTTP dispatch and
header observation. Separate clients can share quota state while retaining their
own transport timeouts and token renewal state. Callers receive decoded JSON or
an error; permits and queue admission remain private implementation details.
JSON decode failures retain their existing retry policy alongside HTTP and
transport failures.

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

The coordinator actor exclusively owns scheduler state. Callers submit owned
attempts and await their individual reply channels. The event loop polls admitted
HTTP operations concurrently, applies completed response feedback, and admits
queued attempts when all constraints allow. JSON decoding and retry backoff stay
in the client's `execute_json` future; each retry rejoins FIFO admission.

Dropping a reply receiver cancels queued or admitted work. Queued cancellation
consumes no quota; admitted cancellation releases outstanding bookkeeping while
preserving the charged timestamp. A cancellation wakeup prevents a removed FIFO
head from delaying later requests behind an admission timer.

Synchronous provider construction does not require a Tokio runtime. Each
`execute_json` call selects its coordinator from the context-owned DashMap
registry using the configured origin and authentication. Hold the returned Arc
locally through the entire retry loop; retries reuse the same channel. Clients
store no coordinator cache. Authentication reconfiguration changes only that
client's identity and token state, preserving existing clones. Registry lookup
replaces a closed actor atomically, including after a Tokio runtime shuts down.

The first registry lookup creates the shared channel and lifecycle record
and spawns its HTTP actor. A closed actor is replaced atomically under the same
key. No `OnceLock` or separate registry worker thread is needed.

The application context constructs one `StacRequestRegistry` and shares its
handle with database handles, so execution contexts and provider initialization
use the same quota registry across queries and sessions. Query contexts do not
create a fresh registry: that would split a remote server's quota across queries.
Standalone providers can receive an explicit shared registry; CLI harvesting
creates one for its operation.

DashMap entry guards protect lookup, creation, replacement, and idle retirement.
Retirement checks the exact coordinator pointer and registry-only ownership
under that same guard, preventing removal of a coordinator handed to a caller or
of a newer replacement. Release every map guard before awaiting or doing HTTP
work. Request quota state stays private to each actor. Coordinators refer weakly
to their registry so dropping its context and clients releases it without a
reference cycle.

All server feedback can lower capacity, irrespective of dispatch order. It cannot
refill capacity until reset, because dispatch order need not match server
processing order. Authorization and transport timeouts are applied at dispatch.
Existing grid geometry and Moka cache matching stay unchanged.

Only the one-second fallback delay and five-minute minimum idle retention are
fixed constants. Advertised request counts, window durations and cooldowns are
not capped. Unrepresentable deadlines are ignored. Every window uses the same
request history, retained for the longest known window or five minutes, whichever
is longer. The limit-th most recent timestamp determines local capacity in O(1)
per policy. Header policies and feedback are parsed together once by the actor, using the
same `Window` type as scheduler state. Each JSON call performs one registry
lookup; all retries use its selected coordinator.

## Validation commands

- `cargo test -p geoengine-services --lib stac -- --test-threads=1` covers the
  provider, coordinator, grid cache, authentication, harvester and discovery.
  Serial test execution avoids interference between tests that install logging
  subscribers; individual scheduling and HTTP regressions still exercise
  concurrent requests explicitly.
- `cargo clippy -p geoengine-services --all-features --all-targets -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`

The alternative implementation steps are recorded in
[the channel coordinator plan](stac-request-coordinator-channels.md).
