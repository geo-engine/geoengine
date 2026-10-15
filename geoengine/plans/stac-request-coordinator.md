# STAC request coordinator implementation plan

## Purpose and scope

All STAC HTTP attempts within one backend process use a FIFO coordinator shared
by server origin (scheme, host, effective port) and full configured authentication
identity (token endpoint, client id, username, password), or anonymous identity.
Collection, provider id, page size, and refreshed access tokens are not identity.
For each server, all anonymous requests share one quota bucket; matching
configured authentication shares another bucket, across all providers and
collections. Different authentications have independent quotas. Cross-origin pagination uses the
anonymous coordinator for that destination and never receives STAC credentials.
OAuth token endpoints and GDAL asset reads remain outside STAC coordination.

## Scheduling

Add a private `coordinator` module with a process-wide registry, secret-safe
identity, and shared scheduler state. Retain registry entries while a client or
attempt holds them, and for at least the longest learned window after activity;
prune unused expired entries on registry lookup. This preserves quota history
across provider reconstruction. Serialize enqueue/admission with FIFO tickets and Tokio notifications,
but allow HTTP attempts to overlap after reserving quota. Learn
headers from one bootstrap response before opening concurrent admission. Reserve
quota before dispatch; release outstanding bookkeeping even on cancellation or
network failure. Do not hold cache locks during queue waits or HTTP calls.

Track all advertised quota windows using bounded timestamp histories. Parse
`x-ratelimit-limit: 10, 10;w=1, 60;w=60`, `remaining: 9`, and `reset: 1`.
Reset is a relative duration in seconds. Locally enforce both windows; observed
remaining/reset additionally accounts for other clients. Concurrent responses
must not replenish quota using stale observations or omit outstanding requests.
Use monotonic deadlines. Missing or malformed headers must not panic or stall
forever. Avoid uncontrolled bursts when quota information is missing.

A 429 updates a shared cooldown, honoring Retry-After (seconds and HTTP date)
and reset, with a one-second fixed fallback when neither is usable. Missing quota headers
use a conservative fallback of ten requests per second after initial discovery. Retry the rejected
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

## Implementation handoff

Implementation is delegated to the requested GPT-6 Luna subagent. The primary
agent reviews its changes and owns any final fixes and validation.

## Review adjustments

Use synchronous mutexes for short scheduler bookkeeping so cancellation cleanup
runs immediately even when an async runtime shuts down. No guard spans an await.
Authorization and transport timeouts are applied when dispatch begins. Existing
grid geometry and Moka cache matching stay unchanged. Trailing commas in existing STAC
mapping test fixtures are repaired so the full STAC suite can validate the feature.

Implementation bounds parsed window histories to 10,000 requests per window,
window durations to one day, and individual 429 cooldowns to five minutes; the
CODE-DE one-second/minute policy is within those bounds. Registry entries remain
for at least five minutes after activity. Initial attempts are queued in arrival
order; a rejected 429 attempt rejoins the FIFO queue for its retry.

## Validation commands

- `cargo test -p geoengine-services --lib stac -- --test-threads=1` covers the
  provider, coordinator, grid cache, authentication, harvester and discovery.
  Serial test execution avoids interference between tests that install logging
  subscribers; individual scheduling and HTTP regressions still exercise
  concurrent requests explicitly.
- `cargo clippy -p geoengine-services --all-features --all-targets -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
