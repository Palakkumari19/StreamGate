# API

Base URL in the default compose stack: `http://localhost:8080` (nginx).

Three surfaces:

- **`/v1/*`** — OpenAI-compatible, authenticated with a tenant API key.
- **`/admin/*`** — operator surface, authenticated with `ADMIN_TOKEN`.
- **`/healthz`, `/readyz`, `/metrics`** — unauthenticated, for infrastructure.

Plus the mock backend's own `/admin/faults`, which is how chaos tests inject failures.

---

## Conventions

**Auth.** `Authorization: Bearer sg_<prefix><secret>` for `/v1`,
`Authorization: Bearer <ADMIN_TOKEN>` for `/admin`.

**Request IDs.** Send `X-Request-Id` and it is echoed on the response and used in every log
line for that request; omit it and the gateway generates a ULID. Always echoed as
`X-Request-Id`.

**Idempotency.** `Idempotency-Key: <opaque>` on a **non-streaming** POST makes the response
replayable for 24 h (same key + same tenant → cached response, `X-Idempotent-Replay: true`).
Streaming requests accept the header but only use it to deduplicate concurrent in-flight
duplicates; a completed stream is not replayed (see [ADR-011](DESIGN_DECISIONS.md#adr-011-idempotency-keys-cover-non-streaming-responses-only)).

**Errors** use the OpenAI error envelope so existing SDKs surface them properly:

```json
{
  "error": {
    "message": "Rate limit exceeded for tenant 'acme': requests per minute",
    "type": "rate_limit_error",
    "code": "rpm_exceeded",
    "param": null,
    "request_id": "01JA7Z…"
  }
}
```

| HTTP | `type` | When |
| --- | --- | --- |
| 400 | `invalid_request_error` | malformed body, unknown field combination, `max_tokens` over cap |
| 401 | `authentication_error` | missing, malformed, unknown or revoked key |
| 403 | `permission_error` | tenant inactive, or model not allowed for tenant |
| 404 | `invalid_request_error` | unknown model |
| 408 | `timeout_error` | upstream did not produce a first token within `FIRST_TOKEN_TIMEOUT` |
| 429 | `rate_limit_error` | `rpm_exceeded` or `tpm_exceeded`, with `Retry-After` |
| 502 | `upstream_error` | backend returned an unusable response after all attempts |
| 503 | `service_unavailable` | no healthy backend, or gateway draining |

---

## `POST /v1/chat/completions`

Request body (subset of the OpenAI schema that StreamGate honours; unknown fields are rejected
rather than silently dropped, so clients learn the truth early):

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `model` | string | required | must resolve to ≥1 registered backend |
| `messages` | array | required | `{role, content}`; roles `system`/`user`/`assistant` |
| `stream` | bool | `false` | `true` → SSE |
| `max_tokens` | int | `512` | capped by `MAX_COMPLETION_TOKENS`; used for the tpm reservation |
| `temperature` | float | `1.0` | forwarded |
| `top_p` | float | `1.0` | forwarded |
| `stop` | string \| array | — | forwarded |
| `stream_options` | object | — | `{"include_usage": true}` → a final usage-only chunk |
| `user` | string | — | opaque, logged for attribution |

### Non-streaming

```bash
curl -s localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $SG_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "mock-small",
    "messages": [{"role":"user","content":"Name three ocean currents."}],
    "max_tokens": 64
  }'
```

```json
{
  "id": "chatcmpl-01JA7Z…",
  "object": "chat.completion",
  "created": 1760000000,
  "model": "mock-small",
  "choices": [{
    "index": 0,
    "message": {"role": "assistant", "content": "The Gulf Stream, the Kuroshio …"},
    "finish_reason": "stop"
  }],
  "usage": {"prompt_tokens": 14, "completion_tokens": 38, "total_tokens": 52}
}
```

Response headers:

```
X-Request-Id: 01JA7Z…
X-StreamGate-Backend: mock-b
X-StreamGate-Attempts: 1
X-RateLimit-Limit-Requests: 60
X-RateLimit-Remaining-Requests: 59
X-RateLimit-Limit-Tokens: 20000
X-RateLimit-Remaining-Tokens: 19948
```

### Streaming (SSE)

```bash
curl -N localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $SG_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-small","stream":true,
       "stream_options":{"include_usage":true},
       "messages":[{"role":"user","content":"Count to five."}]}'
```

`Content-Type: text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`,
`X-Accel-Buffering: no`.

```
data: {"id":"chatcmpl-01JA7Z…","object":"chat.completion.chunk","created":1760000000,"model":"mock-small","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}

data: {"id":"chatcmpl-01JA7Z…","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"One"},"finish_reason":null}]}

data: {"id":"chatcmpl-01JA7Z…","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":" two"},"finish_reason":null}]}

data: {"id":"chatcmpl-01JA7Z…","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"chatcmpl-01JA7Z…","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":5,"total_tokens":16}}

data: [DONE]
```

**Mid-stream failure** (after the first token; see the ADR for why there is no retry):

```
data: {"id":"chatcmpl-…","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"The cap"},"finish_reason":null}]}

data: {"error":{"message":"Upstream backend 'mock-a' closed the stream after 3 chunks","type":"upstream_error","code":"stream_interrupted","request_id":"01JA7Z…"}}

data: [DONE]
```

A comment line (`: keepalive`) is emitted every `SSE_KEEPALIVE_SECONDS` while waiting on a slow
upstream, so proxies and load balancers do not time the connection out during a long prefill.

### `429` response

```
HTTP/1.1 429 Too Many Requests
Retry-After: 4
X-RateLimit-Limit-Requests: 60
X-RateLimit-Remaining-Requests: 0
X-RateLimit-Reset-Requests: 4
```

```json
{"error":{"message":"Rate limit exceeded for tenant 'acme': requests per minute","type":"rate_limit_error","code":"rpm_exceeded","request_id":"01JA7Z…"}}
```

---

## `GET /v1/models`

Returns models the caller's tenant may use, derived from the backend registry (union of
`models` across backends with at least one non-open breaker).

```json
{"object":"list","data":[
  {"id":"mock-small","object":"model","owned_by":"streamgate","created":1760000000},
  {"id":"mock-large","object":"model","owned_by":"streamgate","created":1760000000}
]}
```

---

## Admin API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/admin/tenants` | create a tenant with limits |
| `GET` | `/admin/tenants` | list tenants |
| `GET` | `/admin/tenants/{tenant_id}` | one tenant with its keys (metadata only) |
| `PATCH` | `/admin/tenants/{tenant_id}` | change limits or `is_active` |
| `POST` | `/admin/tenants/{tenant_id}/keys` | issue a key — **plaintext returned once** |
| `DELETE` | `/admin/keys/{key_id}` | revoke a key (sets `revoked_at`) |
| `GET` | `/admin/tenants/{tenant_id}/usage?from=&to=` | aggregated usage + cost |
| `GET` | `/admin/backends` | live view: health, breaker state, in-flight, EWMA |
| `POST` | `/admin/backends/{name}/drain` | stop routing new requests to a backend (manual cordon) |
| `POST` | `/admin/backends/{name}/undrain` | undo the above |

### Create tenant

```bash
curl -s -X POST localhost:8080/admin/tenants \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"acme","rpm_limit":60,"rpm_burst":20,"tpm_limit":20000,"tpm_burst":5000}'
```

```json
{"id":"8f1c…","name":"acme","rpm_limit":60,"rpm_burst":20,
 "tpm_limit":20000,"tpm_burst":5000,"is_active":true,"created_at":"2026-10-10T09:00:00Z"}
```

### Issue a key

```json
{"id":"b22e…","prefix":"a1b2c3d4","label":"laptop",
 "key":"sg_a1b2c3d4K7f2QpZ…","created_at":"2026-10-10T09:00:05Z",
 "warning":"Store this key now. It cannot be retrieved again."}
```

### Usage

```json
{"tenant_id":"8f1c…","from":"2026-10-01T00:00:00Z","to":"2026-10-10T00:00:00Z",
 "requests":1842,"prompt_tokens":210934,"completion_tokens":512003,
 "estimated_fraction":0.07,"cost_usd":"1.4821",
 "by_model":[{"model":"mock-small","requests":1800,"cost_usd":"1.2100"}],
 "by_status":{"ok":1790,"rate_limited":31,"disconnected":14,"error":7}}
```

### Backends

```json
{"backends":[
  {"name":"mock-a","base_url":"http://mock-llm-a:9000","healthy":true,
   "breaker":{"state":"closed","failures":0,"opened_at":null},
   "in_flight":3,"ewma_latency_ms":412.5,"drained":false,"models":["mock-small"]},
  {"name":"mock-b","healthy":false,
   "breaker":{"state":"open","failures":5,"opened_at":"2026-10-10T09:14:02Z"},
   "in_flight":0,"ewma_latency_ms":9100.0,"drained":false,"models":["mock-small"]}
]}
```

---

## Infrastructure endpoints

| Path | Meaning |
| --- | --- |
| `GET /healthz` | liveness: the process is up. Never touches Redis or Postgres — otherwise a Redis blip gets your healthy gateways killed |
| `GET /readyz` | readiness: config loaded, Postgres reachable, ≥1 routable backend. Returns `503` while draining so nginx stops sending traffic |
| `GET /metrics` | Prometheus text format |

---

## Mock backend API (per backend, port 9000)

Serves `POST /v1/chat/completions` (streaming and not) and `GET /healthz`, plus a fault
injection surface — this is what makes chaos testing scriptable instead of manual.

```bash
# make this backend slow: 2s before first token, 5 tokens/sec
curl -X POST localhost:9001/admin/faults -H 'Content-Type: application/json' \
  -d '{"ttft_ms":2000,"tokens_per_sec":5}'

# fail 50% of requests with 503
curl -X POST localhost:9001/admin/faults -d '{"error_rate":0.5,"error_status":503}'

# accept the request, stream 3 chunks, then kill the connection
curl -X POST localhost:9001/admin/faults -d '{"die_after_chunks":3}'

# hang without responding (tests first-token timeout)
curl -X POST localhost:9001/admin/faults -d '{"hang":true}'

# back to normal
curl -X DELETE localhost:9001/admin/faults
```

`GET /healthz` on a mock returns `{"status":"ok","in_flight":2,"queue_depth":0}` — the
`queue_depth` field is what the EWMA strategy folds into its score, mimicking what a real vLLM
`/metrics` endpoint exposes.
