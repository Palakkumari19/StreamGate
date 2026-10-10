# Design decisions (ADRs)

Each record: **context → decision → alternatives considered → consequences**. Status is
`accepted` unless noted. These are decisions made in Phase 0; any later reversal gets a new ADR
rather than an edit, so the history stays honest.

| # | Decision | Status |
| --- | --- | --- |
| [001](#adr-001-openai-compatible-api-surface) | OpenAI-compatible API surface | accepted |
| [002](#adr-002-sse-for-streaming-not-websockets-or-http2-push) | SSE for streaming, not WebSockets | accepted |
| [003](#adr-003-failover-only-before-the-first-token) | Failover only before the first token | accepted |
| [004](#adr-004-routing-tier-as-an-in-process-library) | Routing tier as an in-process library | accepted |
| [005](#adr-005-redis-lua-token-bucket-for-distributed-rate-limiting) | Redis Lua token bucket | accepted |
| [006](#adr-006-fail-open-when-redis-is-unavailable) | Fail **open** when Redis is unavailable | accepted |
| [007](#adr-007-per-replica-circuit-breakers) | Per-replica circuit breakers | accepted |
| [008](#adr-008-reserve-then-reconcile-token-accounting) | Reserve-then-reconcile token accounting | accepted |
| [009](#adr-009-argon2id-key-hashing-with-a-searchable-prefix) | Argon2id key hashing with a searchable prefix | accepted |
| [010](#adr-010-usage-records-written-off-the-response-path) | Usage records written off the response path | accepted |
| [011](#adr-011-idempotency-keys-cover-non-streaming-responses-only) | Idempotency keys cover non-streaming only | accepted |
| [012](#adr-012-a-configurable-mock-backend-is-a-first-class-component) | Mock backend is a first-class component | accepted |
| [013](#adr-013-postgres--redis-rather-than-one-store) | Postgres **and** Redis, not one store | accepted |
| [014](#adr-014-ewma-over-raw-last-latency-and-over-p99) | EWMA over raw last-latency | accepted |
| [015](#adr-015-active-health-checks-in-addition-to-passive-breaker-signals) | Active health checks as well as passive signals | accepted |
| [016](#adr-016-prometheus-pull-metrics-with-bounded-label-cardinality) | Prometheus pull with bounded label cardinality | accepted |

---

## ADR-001: OpenAI-compatible API surface

**Context.** The gateway needs a client contract. It could expose its own cleaner protocol, or
mimic the OpenAI chat completions API.

**Decision.** Implement `POST /v1/chat/completions` and `GET /v1/models` with OpenAI-compatible
request and response shapes, including the SSE chunk format and `data: [DONE]` sentinel.

**Alternatives.**
- *A bespoke REST API.* Cleaner (no `choices[0].message` awkwardness, no `[DONE]` sentinel that
  is not valid JSON), but every client would need a custom SDK.
- *gRPC bidirectional streaming.* Better flow control and typed contracts, far worse
  ecosystem fit — no `curl`, no browser, no existing SDK, and gRPC-web proxying is its own
  project.

**Consequences.** Any OpenAI SDK, LangChain, or `curl` works unchanged by pointing `base_url`
at StreamGate — that demoability is most of the project's value. The cost is inheriting the
format's warts: a non-JSON terminator, usage reporting that is optional in streams, and no
standard way to signal a mid-stream error (hence the in-band error event in
[ADR-003](#adr-003-failover-only-before-the-first-token)). Unknown request fields are
**rejected**, not ignored, so clients do not silently believe an unsupported parameter was
honoured.

---

## ADR-002: SSE for streaming, not WebSockets or HTTP/2 push

**Context.** Tokens must reach the client incrementally.

**Decision.** Server-Sent Events over HTTP/1.1 chunked transfer encoding.

**Alternatives.**
- *WebSockets.* Full duplex, which this workload does not need — the client says one thing then
  listens. Adds an upgrade handshake, a framing layer, its own keepalive/ping discipline, and
  loses plain HTTP semantics (status codes, caching headers, trivial proxying).
- *HTTP/2 streams.* Real multiplexing and proper flow control, but the complexity lands on the
  proxy configuration and debuggability, and the OpenAI ecosystem speaks SSE.
- *Long polling.* Latency per chunk makes streaming pointless.

**Consequences.** `curl -N` is a complete debugging tool, which matters enormously for a
teaching project. Downsides to know: SSE is unidirectional, so client cancellation is signalled
only by TCP disconnect (which is exactly why [§3.4](ARCHITECTURE.md#34-client-disconnect)
matters); intermediary buffering silently ruins it, so `proxy_buffering off` is mandatory; and
browsers cap concurrent connections per origin on HTTP/1.1.

---

## ADR-003: Failover only before the first token

**Context.** A backend can fail at connect time, before the first byte, or halfway through
generating. Retry semantics differ fundamentally across those cases.

**Decision.** Retries and failover are permitted **until the first content chunk has been
flushed to the client**. After that, a failure is terminal: emit an in-band SSE error event,
then `data: [DONE]`, and mark the usage record `truncated`.

**Alternatives.**
- *Retry mid-stream from scratch.* The client has already rendered text; a fresh generation
  produces different text. The result is visibly corrupt output.
- *Resumable streams* — feed the delivered partial completion back as context and ask a second
  backend to continue. Technically possible, but it changes request semantics, pays prefill
  twice, cannot guarantee coherence, and is invisible to the client in a way that makes
  debugging awful. Reasonable as an opt-in product feature; wrong as a default.
- *Buffer the whole completion server-side, then stream it.* Makes failover trivially
  retryable and destroys TTFT — i.e. deletes the feature.

**Consequences.** Failures are honest and observable, and `streamgate_stream_interrupted_total`
becomes a meaningful SLO signal. Clients must handle a terminal error event mid-stream, which
is documented in [API.md](API.md). The first-token boundary also means a **timeout before the
first token is retryable** — so `FIRST_TOKEN_TIMEOUT` is a separate, shorter timeout than the
overall stream timeout. That distinction is the practical heart of this ADR.

---

## ADR-004: Routing tier as an in-process library

**Context.** The routing tier could be its own service (a true middle tier) or a library linked
into each gateway replica.

**Decision.** Library, inside the gateway process. Replicas share load signals through Redis.

**Alternatives.**
- *Dedicated router service.* Globally consistent load view and one place to deploy routing
  changes; costs a network hop on every request and — if it proxies the stream — on every
  chunk, plus it becomes a new SPOF that must itself be made HA.
- *nginx/Envoy doing the load balancing.* Battle-tested least-conn and health checking, but it
  cannot see LLM-specific signals (queue depth, EWMA of *generation* latency, per-tenant
  policy) and cannot implement the first-token failover rule, which needs application
  knowledge of the SSE body.

**Consequences.** The hot path stays at one hop. Routing logic is directly unit-testable as
pure functions. The price is that each replica has a slightly different view of the world
(stale by the stats-cache interval) and health-check traffic multiplies by replica count. Both
are acceptable at this scale and are called out in [RISKS.md](RISKS.md).

---

## ADR-005: Redis Lua token bucket for distributed rate limiting

**Context.** With N stateless gateway replicas, a per-process limiter lets a tenant exceed its
limit by roughly N×. The check must be atomic and shared.

**Decision.** A token bucket per (tenant, dimension) stored as a Redis hash `{tokens, ts}`,
manipulated by one `EVALSHA` Lua script that reads, refills by elapsed time, decides, writes,
and sets a TTL — all inside Redis's single-threaded execution, so it is atomic by construction.
Redis `TIME` is the clock.

**Alternatives.**
- *`INCR` + `EXPIRE` fixed window.* Two commands (raceable unless pipelined with care) and the
  classic boundary problem: 2× the limit across a window edge.
- *Sorted-set sliding window log.* Exact, but O(n) memory per tenant per window and heavy
  trimming — overkill for per-minute limits.
- *`WATCH`/`MULTI` optimistic transactions.* Correct, but retries under contention, and three
  round trips instead of one.
- *Local limiters with periodic reconciliation.* Fast, approximate, and allows bursts beyond
  the limit — the exact bug this ADR exists to prevent.

**Consequences.** One round trip per dimension (two per request: rpm, tpm), provably correct
across replicas, and testable by pointing two gateway replicas at one Redis and asserting the
total admitted count. Redis becomes a hot dependency — see
[ADR-006](#adr-006-fail-open-when-redis-is-unavailable). The script must be kept
deterministic and side-effect-light because it blocks the whole Redis server while it runs.

---

## ADR-006: Fail **open** when Redis is unavailable

**Context.** If Redis is down, the gateway cannot evaluate rate limits. Serve anyway, or reject?

**Decision.** Fail open: serve the request, log at `warning`, and increment
`streamgate_ratelimit_degraded_total`. A Grafana alert fires on that counter.

**Alternatives.**
- *Fail closed (reject everything).* Turns a rate-limiter outage into a total platform outage.
  Correct for a payments authorisation service; wrong for a throughput limiter whose purpose is
  protecting capacity, especially when the breaker and backend concurrency limits still provide
  a backstop.
- *Fall back to per-replica local limits.* Tempting middle ground, and genuinely better in
  production. Rejected here for honesty: it adds a second limiter implementation and a second
  set of semantics to explain and test. Recorded as future work, not quietly skipped.

**Consequences.** Availability is preserved; a Redis outage means limits are temporarily
unenforced, which a malicious tenant could exploit if they could detect it. This is a
*deliberate, documented* trade, and being able to articulate it is the point.

---

## ADR-007: Per-replica circuit breakers

**Context.** Breaker state (failure counts, open/closed) could be shared in Redis or kept local
to each replica.

**Decision.** Local, in-memory, per replica. Each gateway learns backend health from its own
observations.

**Alternatives.**
- *Shared breaker state in Redis.* All replicas trip together, so a bad backend is shunned
  instantly and uniformly. But it adds a Redis round trip to the hot path, couples
  availability of routing to Redis, and makes one replica's local network problem poison routing
  for everyone — a false positive becomes cluster-wide.
- *Gossip between replicas.* Real complexity for marginal benefit at N=2.

**Consequences.** Breakers trip independently, so the cluster converges on a bad backend more
slowly (each replica needs its own `failure_threshold` observations) and `/admin/backends`
reports the view of whichever replica answered — which is confusing until you know it, so the
runbook says so explicitly. In exchange: no hot-path dependency, and partial failures stay
partial. Observed breaker state is still *published* to Redis for dashboards; it is just never
read back for decisions.

---

## ADR-008: Reserve-then-reconcile token accounting

**Context.** A tokens-per-minute limit must be charged before generation, but completion token
count is unknowable until generation ends.

**Decision.** Reserve `estimate(prompt) + max_tokens` from the tpm bucket before dispatch. When
the request finishes, refund `reserved − actual` (clamped at ≥0) by adding tokens back to the
bucket, capped at bucket capacity.

**Alternatives.**
- *Charge only prompt tokens.* Lets a tenant generate unbounded output within their limit —
  which inverts the point, since completion tokens are the expensive ones.
- *Charge after the fact only.* Permits a huge burst before any limit is felt.
- *Charge a flat per-request cost.* Punishes small requests, invisible to large ones.

**Consequences.** Limits are conservative: a tenant who sets `max_tokens: 4096` and generates
50 tokens is briefly charged for 4096, so they may get a `429` slightly early. The refund makes
this self-correcting within the window. The alternative (optimism) over-admits and then
overshoots the limit, which is worse for capacity protection. The refund path must run in a
`finally` block, including on client disconnect, or buckets leak and tenants are
under-served — a good bug to be able to describe in an interview.

---

## ADR-009: Argon2id key hashing with a searchable prefix

**Context.** API keys must be verifiable without being recoverable, and lookup must be O(1).

**Decision.** Key format `sg_<8-char prefix><32-char secret>`. The prefix is stored plaintext
and uniquely indexed; the full key is hashed with Argon2id. Lookup is: parse prefix → indexed
fetch of one row → Argon2 verify in a thread pool → cache the result for 30 s.

**Alternatives.**
- *Hash the whole key with bcrypt/Argon2 and no prefix.* Verification would require trying every
  row, because password hashes are salted and therefore unsearchable. O(n) Argon2 verifications
  per request: unusable.
- *Unsalted SHA-256 of the key, indexed directly.* This is actually defensible for
  high-entropy random keys (no dictionary attack to speed up) and is what several large
  providers do. Argon2 was chosen anyway for defence in depth and because it is the answer
  interviewers expect — but it needs the prefix *and* the cache to be viable, which is the
  interesting part.
- *Signed tokens (JWT/HMAC) with no DB lookup.* Stateless and fast, but revocation requires a
  denylist, which puts the lookup back.

**Consequences.** Argon2 is intentionally slow (tens of ms), so it must never run on the event
loop and must be cached — the cache TTL is then the revocation lag, documented as ≤30 s. The
prefix is a non-secret correlation handle, which is also handy for logs: you can log
`key_prefix` safely and never the key.

---

## ADR-010: Usage records written off the response path

**Context.** Every request produces a usage row. Writing it synchronously adds DB latency to
each response and makes Postgres availability a request-path dependency.

**Decision.** Build a `RequestOutcome` in a `finally` block, push it to a bounded in-memory
queue, and have a background task batch-insert. On overflow, drop and increment
`streamgate_usage_records_dropped_total`.

**Alternatives.**
- *Synchronous insert.* Simple and lossless; adds latency and couples availability. Unacceptable
  on the streaming path, where it would also delay connection teardown.
- *Write to Redis, drain to Postgres with a separate worker.* Survives gateway restarts — a
  genuinely better design. Rejected for scope: it introduces a second process to operate. Noted
  as future work.
- *Fire-and-forget `asyncio.create_task` per record.* Unbounded task growth under load, and
  tasks that can be garbage-collected mid-flight.

**Consequences.** Usage data is *best effort*: a SIGKILLed replica loses up to one batch. For
billing-grade accounting that is not good enough, and the README says so rather than implying
the numbers are authoritative. Batching also means the admin usage endpoint can lag by up to
one flush interval.

---

## ADR-011: Idempotency keys cover non-streaming responses only

**Context.** Clients retry. A retried `POST` should not double-charge or double-generate.

**Decision.** `Idempotency-Key` on a non-streaming request caches the full response body in
Redis for 24 h, keyed by `(tenant, key)`, and replays it on a repeat. For streaming requests the
key is only used to reject a *concurrent* duplicate (`409`-style conflict); a completed stream
is not replayed.

**Alternatives.**
- *Buffer and cache streamed responses too.* Would require storing the whole completion to
  replay it, which both costs memory proportional to output and contradicts the decision not to
  persist message content ([DATA_MODELS.md §1](DATA_MODELS.md#1-postgres-durable)).
- *No idempotency at all.* Honest, but it means a client-side retry after a timeout can bill a
  tenant twice for work they never received — and "how do you make a non-idempotent POST safe to
  retry?" is a question worth having an answer to.

**Consequences.** Streaming retries are the client's problem, stated plainly in the docs. The
stored response includes the original `request_id`, and the replay is flagged with
`X-Idempotent-Replay: true` so the behaviour is visible rather than magic. A key reused with a
*different* request body returns `422` rather than silently serving the old answer.

---

## ADR-012: A configurable mock backend is a first-class component

**Context.** Testing a gateway requires backends that fail in specific ways on demand. Real
models are slow, nondeterministic, need a GPU, and cannot be told to die after three chunks.

**Decision.** Ship `services/mockllm`: an OpenAI-compatible server with configurable TTFT,
tokens/sec, jitter, and a runtime fault-injection endpoint (`error_rate`, `die_after_chunks`,
`hang`, slow mode). Three instances run in the default stack. Ollama is an optional overlay.

**Alternatives.**
- *Only real backends (Ollama/vLLM).* No CI (no GPU runners), no deterministic tests, no way to
  reproduce a mid-stream death.
- *`unittest.mock`-style stubs inside the test process.* Cannot exercise real sockets, real
  SSE framing, real TCP resets, or nginx — which is where the actual bugs live.

**Consequences.** `docker compose up` works on any laptop with no model download, and chaos
scenarios become shell one-liners. The risk is testing against a fiction: the mock does not
reproduce real KV-cache pressure, batching effects, or the way real TTFT degrades
superlinearly with concurrency. So the benchmark doc requires at least one run against a real
Ollama backend before any performance claim is made.

---

## ADR-013: Postgres **and** Redis, rather than one store

**Context.** Two datastores is operational overhead for a student project.

**Decision.** Keep both: Postgres for durable relational data (tenants, keys, usage), Redis for
hot shared ephemeral state (buckets, backend stats, idempotency).

**Alternatives.**
- *Postgres only.* Token buckets in Postgres means a row lock per request on the hot path, plus
  `SELECT … FOR UPDATE` contention and vacuum pressure from high-churn updates. Workable at low
  rates; the wrong shape for a rate limiter.
- *Redis only.* Durability for billing records depends on AOF/RDB configuration, and aggregation
  queries ("usage by model last month") become application code instead of SQL.

**Consequences.** Two dependencies to run and reason about, but each is used for what it is
good at, and the failure modes are *independent* — which is why the degradation matrix has
different answers for "Redis down" and "Postgres down". Being able to explain that each store
earns its place is better than minimising the service count.

---

## ADR-014: EWMA over raw last-latency (and over p99)

**Context.** The latency-aware strategy needs a per-backend latency signal.

**Decision.** Exponentially weighted moving average of observed request latency, updated on each
completion: `ewma = α·sample + (1−α)·ewma`, with α configurable (default 0.3). The routing score
combines it with live in-flight count and reported queue depth.

**Alternatives.**
- *Last observed latency.* Maximally reactive and maximally noisy — one slow outlier removes a
  healthy backend from rotation.
- *Windowed p99.* More faithful to tail behaviour, but needs a histogram or reservoir per
  backend, more memory, and more code; and it reacts slowly, which is wrong for a signal that
  drives routing.
- *Simple moving average over a fixed window.* Needs a ring buffer per backend and weights stale
  samples equally with fresh ones.

**Consequences.** One float per backend, O(1) update, tunable responsiveness via α — and a known
pathology worth stating: a backend that has been idle keeps its stale EWMA, and a backend whose
breaker just closed has no fresh data. Mitigations: decay toward the fleet mean when idle, and
treat half-open probes as unweighted. Also note EWMA alone causes **herding** (every replica
picks the same "fastest" backend at once); including in-flight count in the score is what damps
that, and power-of-two-choices is the recorded next step.

---

## ADR-015: Active health checks in addition to passive breaker signals

**Context.** The breaker already learns about failures from real traffic. Is a separate
health-check loop redundant?

**Decision.** No — run both. A background task probes each backend's `/healthz` on an interval,
with its own timeout and consecutive-failure threshold, and marks `healthy`. The breaker handles
request-path failures.

**Alternatives.**
- *Passive only.* A backend that is down receives no traffic (because the breaker opened), so
  nothing ever discovers that it recovered except the half-open probe — which uses a *real
  user's request* as the guinea pig. Also, a cold-started backend gets traffic before it is
  ready.
- *Active only.* `/healthz` returning 200 says nothing about whether generation works or how
  loaded the backend is; health endpoints lie routinely.

**Consequences.** Recovery is detected without sacrificing a user request, and the health
payload is also where queue depth comes from for routing. Cost: N replicas × M backends probes
per interval, and two sources of truth for "is this backend usable", which the registry
combines as `usable = healthy AND breaker.allows() AND NOT drained`. Keeping that single
predicate in one place is what stops this from becoming confusing.

---

## ADR-016: Prometheus pull metrics with bounded label cardinality

**Context.** The gateway needs TTFT, latency, in-flight, breaker state and rate-limit metrics.

**Decision.** `prometheus_client` with a per-process registry scraped at `/metrics`. Labels are
restricted to bounded sets: `backend`, `model`, `status_class`, `strategy`, and `tenant` **only
on a small number of counters**. Never `request_id`, never raw paths, never user strings.

**Alternatives.**
- *StatsD/push gateway.* Needed for short-lived jobs; a long-lived server is the pull model's
  home turf, and pull gives you target health for free.
- *OpenTelemetry metrics + traces.* Strictly richer, and traces would be genuinely useful for a
  multi-hop retry story. Deferred: it is a second mental model to learn and a collector to run.
  Future work.
- *Label everything by tenant.* Convenient dashboards, unbounded cardinality — the standard way
  people take down their own Prometheus. Per-tenant detail belongs in Postgres, which is exactly
  what `usage_records` is for.

**Consequences.** Histograms need explicit buckets chosen for LLM latencies (TTFT in the tens
of ms to seconds; total latency up to minutes) because the client-library defaults assume
sub-second web requests. With N replicas, `/metrics` is per-process, so dashboards must
aggregate (`sum by (backend)`), and in-flight gauges are per-replica — a detail the runbook
spells out so nobody misreads a dashboard.
