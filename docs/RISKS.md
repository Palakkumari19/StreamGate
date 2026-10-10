# Risk register

Two kinds of risk matter here, and they are easy to conflate:

- **Build risks** — things that will eat my time or make the project not finish.
- **System risks** — ways the running system is wrong, fragile, or dishonest.

Severity is `impact × likelihood` for *this project*, not for a hypothetical production
deployment. Each risk names the phase where it gets addressed or accepted, so nothing is quietly
forgotten.

## Build risks

| ID | Risk | Sev | Mitigation | Phase |
| --- | --- | --- | --- | --- |
| B-01 | Scope creep — adding RAG, agents, a UI, Kubernetes | **High** | Scope is frozen by the phase list. Anything else goes to README "future work" and stays there | all |
| B-02 | Streaming + disconnect handling is the hardest part and sits in Phase 2, before there are tests or metrics to debug with | **High** | Build the non-streaming path first, then streaming, then disconnect. Verify disconnects with `curl -N` + Ctrl-C and an upstream log line, before moving on | 2 |
| B-03 | Docker Compose environment drift — works on my machine, not in CI | Med | One compose file is the only way to run it, including locally. CI runs the same `docker compose up`. Pin image tags, never `latest` | 1, 8 |
| B-04 | Async bugs that only appear under concurrency (leaked tasks, unawaited coroutines, cancellation swallowed by a bare `except`) | **High** | `asyncio` debug mode in dev, ban bare `except:`, always `except asyncio.CancelledError: raise`, and assert in tests that in-flight counters return to zero | 2, 5 |
| B-05 | Alembic migrations drifting from the models | Med | CI step that runs `alembic upgrade head` on an empty DB then checks `alembic revision --autogenerate` produces an empty diff | 3, 8 |
| B-06 | Tests that depend on real timing (`sleep`) become flaky in CI | Med | Inject a clock into the breaker and limiter; unit tests advance a fake clock. Only integration tests use real time, with generous bounds | 4, 5 |
| B-07 | Benchmarks run on a laptop with other apps open, producing numbers that mean nothing | Med | BENCHMARKS.md mandates recorded hardware, concurrency, duration, and ≥3 runs; results that cannot be reproduced do not go in the README | 7 |
| B-08 | Not actually understanding the code well enough to defend it | **High** | Every phase ends with interview questions; LEARNING_RESOURCES.md runs alongside; "what I learned" in README is written in my own words, not generated | all |

## System risks

| ID | Risk | Sev | Mitigation / accepted position | Phase |
| --- | --- | --- | --- | --- |
| R-01 | **Leaked upstream connections** on client disconnect — backends keep generating for nobody, silently destroying capacity under load | **High** | Close the upstream response in a `finally`; integration test asserts the backend sees the disconnect; `streamgate_client_disconnects_total` metric | 2, 6 |
| R-02 | Rate limiting incorrect across replicas (the bug that motivates the whole Redis design) | **High** | Atomic Lua script; test fires N concurrent requests at 2 replicas and asserts admitted count ≤ burst + ε | 4 |
| R-03 | Redis as a single point of failure on the hot path | **High** | Fail open ([ADR-006](DESIGN_DECISIONS.md#adr-006-fail-open-when-redis-is-unavailable)); degraded counter + alert. **Accepted**: single-node Redis, no HA story | 4, 6 |
| R-04 | Rate-limit reservation leak — the tpm refund is skipped on an error path, so buckets never recover and a tenant is throttled forever | **High** | Refund in `finally`, one code path for every outcome; unit test for each terminal state (ok / error / disconnect / truncated) | 4 |
| R-05 | In-flight counter drift in Redis after a SIGKILLed replica, poisoning least-in-flight routing permanently | Med | Each replica tracks its own contribution and decrements on graceful shutdown; a periodic reconciliation clamps counters to a sanity bound. **Residual drift accepted** and documented in the runbook | 5, 8 |
| R-06 | Thundering herd / herding — every replica's EWMA agrees on the "fastest" backend and they all pile onto it simultaneously | Med | Score blends in-flight with latency; jitter in tie-breaks. Power-of-two-choices recorded as the proper fix (future work) | 5 |
| R-07 | Breaker flapping — threshold too low or cooldown too short, so a backend oscillates open/closed under moderate load | Med | Require consecutive failures (not a ratio over all time), half-open admits a single probe, exponential cooldown growth on repeated trips; unit tests cover the full state machine | 5 |
| R-08 | Retry storms — retries amplify load on an already-struggling fleet and turn a brownout into an outage | **High** | Hard cap on attempts (default 2), full jitter backoff, retries only for connect/pre-first-token failures, and the breaker removes failing backends from the candidate set entirely | 5 |
| R-09 | Mid-stream failure handled dishonestly (silent truncation that looks like a complete answer) | **High** | Terminal in-band error event + `[DONE]`, usage status `truncated`, dedicated metric. [ADR-003](DESIGN_DECISIONS.md#adr-003-failover-only-before-the-first-token) | 2, 5 |
| R-10 | Token counts wrong, so cost accounting is fiction | Med | Prefer backend-reported usage; fall back to a tokenizer estimate and set `tokens_estimated`; surface `estimated_fraction` in the usage API so the uncertainty is visible | 6 |
| R-11 | Argon2 verification blocking the event loop, turning auth into a throughput ceiling | **High** | Hash in a thread pool + 30 s key cache; load test specifically with many distinct keys to see the cold-cache cost | 3, 7 |
| R-12 | Metric cardinality explosion taking out Prometheus | Med | Bounded label sets only, reviewed per metric; no `request_id`, no raw paths, tenant labels on counters only ([ADR-016](DESIGN_DECISIONS.md#adr-016-prometheus-pull-metrics-with-bounded-label-cardinality)) | 6 |
| R-13 | nginx buffering SSE, making streaming look broken (or worse, look fine locally and break behind a proxy) | Med | `proxy_buffering off`, `proxy_read_timeout` raised, `X-Accel-Buffering: no` on responses; integration test measures TTFT *through nginx*, not against the gateway directly | 1, 2 |
| R-14 | Graceful shutdown that is not graceful — SIGTERM kills in-flight streams | Med | Lifespan handler flips readiness to `503`, waits for in-flight to reach zero up to `DRAIN_TIMEOUT`, then cancels; chaos test kills a replica mid-stream and records what the client sees | 8 |
| R-15 | Secret leakage — API keys or admin tokens in logs, error bodies, or metric labels | **High** | Log only `key_prefix`; a redaction filter in the logging setup; a test that greps captured log output for `sg_` | 1, 3 |
| R-16 | Admin API exposed with a weak shared token | Med | `ADMIN_TOKEN` required, constant-time compare, no default value (startup fails if unset). **Accepted limitation**: not real IAM; README says so | 3 |
| R-17 | Usage records lost on crash, so billing data is incomplete ([ADR-010](DESIGN_DECISIONS.md#adr-010-usage-records-written-off-the-response-path)) | Low | Bounded queue + drop counter + flush on graceful shutdown. **Accepted**: best-effort, stated in README limitations | 6, 8 |
| R-18 | Mock backend behaviour diverges from real inference, so conclusions do not transfer | Med | At least one benchmark run against real Ollama before any performance claim; mock limitations stated in BENCHMARKS.md | 7 |
| R-19 | Head-of-line blocking from one tenant's long streams occupying all backend concurrency | Med | **Accepted for now**: no per-tenant admission control. Visible via in-flight metrics; per-tenant concurrency caps are the first item in future work | — |
| R-20 | Unbounded request bodies or `max_tokens` used as a cheap DoS | Med | Max body size at nginx, `max_tokens` capped server-side, message count/length limits in validation | 2 |

## What I am explicitly *not* solving

Naming these matters as much as the mitigations — an unbounded problem statement is how portfolio
projects die.

- No multi-region, no Redis/Postgres HA, no cross-region failover.
- No real IAM, OAuth, or per-key scopes beyond tenant identity.
- No continuous batching or KV-cache-aware scheduling — that belongs inside vLLM, not in front
  of it.
- No prompt/response storage, so no caching of completions and no content moderation hooks.
- No autoscaling of backends; the fleet is static and health-checked.
- No TLS termination in the demo stack (nginx serves plain HTTP on localhost).
