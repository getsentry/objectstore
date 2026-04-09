# Server Architecture

The objectstore server is an [axum](https://docs.rs/axum)-based HTTP server
that exposes the [`objectstore_service`] storage layer to clients. It handles
authentication, authorization, rate limiting, and traffic control on top of the
core storage operations.

## Endpoints

All object operations live under the `/v1/` prefix:

| Method   | Path                                      | Description                  |
|----------|-------------------------------------------|------------------------------|
| `POST`   | `/v1/objects/{usecase}/{scopes}/`         | Insert with server-generated key |
| `GET`    | `/v1/objects/{usecase}/{scopes}/{key}`    | Retrieve object              |
| `HEAD`   | `/v1/objects/{usecase}/{scopes}/{key}`    | Retrieve metadata only       |
| `PUT`    | `/v1/objects/{usecase}/{scopes}/{key}`    | Insert or overwrite with key |
| `DELETE` | `/v1/objects/{usecase}/{scopes}/{key}`    | Delete object                |
| `POST`   | `/v1/objects:batch/{usecase}/{scopes}/`   | Batch operations (multipart) |

Scopes are encoded in the URL path using Matrix URI syntax:
`org=123;project=456`. An underscore (`_`) represents empty scopes.

### Internal Endpoints

Internal endpoints are exempt from authentication, rate limiting, and the web
concurrency limit so they remain available when the server is under load.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe (always returns 200) |
| `GET` | `/ready` | Readiness probe (returns 503 when `/tmp/objectstore.down` exists, enabling graceful drain) |
| `GET` | `/keda` | Prometheus text-format gauges for KEDA autoscaling (see [KEDA Metrics](#keda-metrics)) |

## Request Flow

A request flows through several layers before reaching the storage service:

1. **Middleware**: metrics collection, in-flight request tracking, panic
   recovery, Sentry transaction tracing, distributed tracing.
2. **Extractors**: path parameters are parsed into an
   [`ObjectId`](objectstore_service::id::ObjectId) or
   [`ObjectContext`](objectstore_service::id::ObjectContext). The auth token is
   read from the `X-Os-Auth` header (preferred) or the standard
   `Authorization` header (fallback), then validated and decoded into an
   [`AuthContext`](auth::AuthContext).
   The optional `x-downstream-service` header is extracted for killswitch
   matching.
3. **Admission control**: [killswitches](killswitches) and
   [rate limits](rate_limits) are checked during extraction. Rejected requests
   never reach the handler.
4. **Handler**: the endpoint handler calls the
   [`AuthAwareService`](auth::AuthAwareService), which checks permissions
   before delegating to the underlying
   [`StorageService`](objectstore_service::StorageService). The service
   enforces its own backpressure before executing the
   operation.
5. **Response**: metadata is mapped to HTTP headers (see
   [`objectstore-types` docs](objectstore_types) for the header mapping) and
   the payload is streamed back.

## Authentication & Authorization

Objectstore uses **JWT tokens with EdDSA signatures** (Ed25519) for
authentication. Auth enforcement is optional and controlled by the
[`auth.enforce`](config) config flag, allowing unauthenticated development
setups.

### Token Structure

Tokens must include:
- **Header**: `kid` (key ID) and `alg: EdDSA`
- **Claims**: `aud: "objectstore"`, `iss: "sentry"` or `"relay"`, `exp`
  (expiration timestamp)
- **Resource claims** (`res`): the usecase and scope values the token grants
  access to (e.g., `{"os:usecase": "attachments", "org": "123"}`)
- **Permissions**: array of granted operations (`object.read`, `object.write`,
  `object.delete`)

### Key Management

The [`PublicKeyDirectory`](auth::PublicKeyDirectory) maps key IDs (`kid`) to
public keys. Each key entry supports multiple key versions for rotation — the
server tries each version when verifying a token. Keys also carry
`max_permissions` that are intersected with the token's claimed permissions,
limiting what any token signed by that key can do.

### Authorization Check

On every operation, [`AuthAwareService`](auth::AuthAwareService) verifies that
the token's scopes and permissions cover the requested
[`ObjectContext`](objectstore_service::id::ObjectContext) and operation type.
Scope values in the token can use wildcards to grant broad access.

## Configuration

Configuration uses [figment](https://docs.rs/figment) for layered merging with
this precedence (highest wins):

1. **Environment variables** — prefixed with `OS__`, using `__` as a nested
   separator. Example: `OS__STORAGE__TYPE=tiered`
2. **YAML file** — passed via the `-c` / `--config` CLI flag
3. **Defaults** — sensible development defaults (local filesystem backend,
   auth disabled)

Key configuration sections:
- `storage` — backend type and connection parameters; use `type: tiered` for
  two-tier routing with `high_volume` and `long_term` sub-backends
- `auth` — key directory and enforcement toggle
- `rate_limits` — throughput and bandwidth limits
- `http` — HTTP layer parameters (concurrency limit)
- `service` — storage service parameters (backend concurrency limit)
- `killswitches` — traffic blocking rules
- `usecases` — per-use-case properties (expiration policy constraints)
- `runtime` — worker threads, metrics interval
- `sentry` / `metrics` / `logging` — observability

See the [`config`] module for the full configuration schema.

## Rate Limiting

Rate limiting operates at two levels:

### Throughput

Throughput limits use **token bucket** rate limiting with configurable burst.
Limits can be set at multiple granularities:

- **Global**: a maximum requests-per-second across all traffic
- **Per-usecase**: a percentage of the global limit allocated to each usecase
- **Per-scope**: a percentage of the global limit for specific scope values
- **Custom rules**: specific RPS or percentage overrides matching usecase/scope
  combinations

### Bandwidth

Bandwidth limiting uses an **exponentially weighted moving average** (EWMA) to
estimate current throughput. Payload streams are wrapped in a
`MeteredPayloadStream` that reports bytes consumed. When the estimated bandwidth
exceeds the configured limit, new requests are rejected.

Like throughput, bandwidth limits can be set at multiple granularities:

- **Global**: a maximum bytes-per-second across all traffic (`global_bps`)
- **Per-usecase**: a percentage of the global limit for each usecase
- **Per-scope**: a percentage of the global limit for each scope value

Each granularity maintains its own EWMA estimator. The `MeteredPayloadStream`
increments all applicable accumulators (global + per-usecase + per-scope) for
every chunk polled. For non-streamed payloads (e.g., batch INSERT where the
size is known upfront), bytes are recorded directly via
[`record_bandwidth`](state::Services::record_bandwidth).

Rate-limited requests receive HTTP 429.

### Web Concurrency Limit

Before requests reach the storage service, a web-tier concurrency limit
protects against connection floods. When the number of in-flight HTTP requests
reaches `http.max_requests` (default: 10,000), new requests are rejected
immediately with HTTP 503. Health and readiness endpoints (`/health`, `/ready`)
are excluded from this limit. Rejections are counted in the
`web.concurrency.rejected` metric.

Direct 503 rejection is preferred over readiness-based backpressure:

- **Instant response and recovery**: direct 503 responds in milliseconds and
  frees capacity the moment any request completes. Readiness probes run on
  periodic intervals, leaving a window of continued overload and wasting
  capacity during recovery.
- **No cascade risk**: multiple pods failing readiness probes simultaneously
  concentrates traffic onto remaining pods. Direct rejection keeps every pod in
  the pool and self-regulating.
- **Correct health semantics**: a busy pod is still *ready* — its dependencies
  are reachable and it can serve traffic. Conflating load with readiness muddies
  alerting and incident response.
- **Environment-independent**: works in any deployment, not just Kubernetes.

### Service Backpressure

Beyond rate limiting and the web concurrency limit, the
[`StorageService`](objectstore_service::StorageService) enforces a second
layer of backpressure through a concurrency limit on in-flight backend
operations, configured via `service.max_concurrency`. When exceeded, requests
receive HTTP 429. See the [service architecture docs](objectstore_service) for
details.

## KEDA Metrics

`GET /keda` serves a Prometheus text-format (version 0.0.4) snapshot of all
four rate-limited resources for use with [KEDA](https://keda.sh/) Prometheus
scalers. The endpoint is exempt from the web concurrency limit and request
metrics so that it remains available when the server is at capacity.

### Exposed Metrics

#### EWMA Gauges

Pre-smoothed rates, self-contained per scrape (no `irate()` arithmetic needed):

| Resource | Utilization | Limit |
|---|---|---|
| Bandwidth | `objectstore_bandwidth_ewma` | `objectstore_bandwidth_limit` (only when `global_bps` is set) |
| Throughput | `objectstore_throughput_ewma` | `objectstore_throughput_limit` (only when `global_rps` is set) |
| HTTP concurrency | `objectstore_requests_in_flight` | `objectstore_requests_limit` |
| Task concurrency | `objectstore_tasks_running` | `objectstore_tasks_limit` |

Throughput uses an EWMA with a 50 ms tick and α = 0.2, matching the existing
bandwidth estimator. The accumulator counts fully admitted requests (requests
that pass all throughput checks).

#### Counters

Monotonically increasing totals since startup; use `irate(counter[window])` in
KEDA queries for an unsmoothed, immediately responsive rate:

| Counter | Description |
|---|---|
| `objectstore_bytes_total` | Total bytes transferred since startup |
| `objectstore_requests_total` | Total admitted requests since startup |

### Example KEDA ScaledObject Triggers

#### Using EWMA gauges (backward-compatible)

Scale on the highest utilization across all four resources:

```yaml
triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus:9090
      query: |
        max(
          objectstore_bandwidth_ewma / objectstore_bandwidth_limit
          or objectstore_throughput_ewma / objectstore_throughput_limit
          or objectstore_requests_in_flight / objectstore_requests_limit
          or objectstore_tasks_running / objectstore_tasks_limit
        )
      threshold: "0.7"
```

#### Using counters with `irate()` (more responsive)

Uses the last two scraped values for an instantaneous rate with no smoothing lag:

```yaml
triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus:9090
      query: |
        max(
          irate(objectstore_bytes_total[2m]) / objectstore_bandwidth_limit
          or irate(objectstore_requests_total[2m]) / objectstore_throughput_limit
          or objectstore_requests_in_flight / objectstore_requests_limit
          or objectstore_tasks_running / objectstore_tasks_limit
        )
      threshold: "0.7"
```

Unconfigured limits produce no series and are excluded from `or` automatically.

## Killswitches

[Killswitches](killswitches::Killswitches) provide emergency traffic blocking
without redeployment. Each killswitch is a set of conditions that, when all
matched, cause requests to be rejected with HTTP 403:

- **Usecase**: exact match on the usecase string
- **Scopes**: all specified scope key-value pairs must be present
- **Service**: a glob pattern matched against the `x-downstream-service`
  request header (e.g., `"relay-*"` to block all relay instances)

A killswitch with no conditions matches all traffic. Multiple killswitches are
evaluated with OR semantics — any match triggers rejection. Killswitches are
checked during request extraction, before the handler runs.

## Use Cases

The `usecases` config block configures per-use-case properties. Use cases
not present in the map are unconstrained. Currently this covers expiration
policy constraints: which policies are permitted and their maximum durations.
Writes that violate the constraints are rejected with HTTP 400. Validation
applies to all insert paths: single-object `POST` and `PUT` endpoints and
batch `INSERT` operations.

See [`usecases`] for the full configuration schema and YAML examples.
