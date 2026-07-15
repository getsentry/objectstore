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
| `GET`    | `/v1/objects/{usecase}/{scopes}/{*key}`   | Retrieve object              |
| `HEAD`   | `/v1/objects/{usecase}/{scopes}/{*key}`   | Retrieve metadata only       |
| `PUT`    | `/v1/objects/{usecase}/{scopes}/{*key}`   | Insert or overwrite with key |
| `DELETE` | `/v1/objects/{usecase}/{scopes}/{*key}`   | Delete object                |
| `POST`   | `/v1/objects:batch/{usecase}/{scopes}/`   | Batch operations (multipart) |

### Multipart Upload Endpoints

| Method    | Path                                                         | Description                          |
|-----------|--------------------------------------------------------------|--------------------------------------|
| `POST`    | `/v1/objects:multipart/{usecase}/{scopes}/`                  | Initiate upload (server-generated key) |
| `PUT`     | `/v1/objects:multipart/{usecase}/{scopes}/{*key}`            | Initiate upload (user-provided key)  |
| `PUT`     | `/v1/objects:multipart:parts/{usecase}/{scopes}/{*key}`      | Upload a part (`uploadId`, `partNumber` query params) |
| `GET`     | `/v1/objects:multipart:parts/{usecase}/{scopes}/{*key}`      | List uploaded parts (`uploadId` query param) |
| `POST`    | `/v1/objects:multipart:complete/{usecase}/{scopes}/{*key}`   | Complete upload (`uploadId` query param) |
| `DELETE`  | `/v1/objects:multipart/{usecase}/{scopes}/{*key}`            | Abort upload (`uploadId` query param) |

The initiate POST endpoint accepts both trailing-slash and non-trailing-slash forms.

The complete endpoint returns `200 OK` immediately, with a streaming body that
will contain the error (if any) as JSON. Whitespace is sent in the streaming body
to keep the connection open.
Clients must parse the body to determine the actual outcome, and not rely on the
status code.

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
2. **Extractors**: extracts object context from path parameters and then
   constructs a service wrapper that authorizes all operations on the target
   resource. Other endpoint-specific extractors also run at this stage.
3. **Admission control**: As part of the extractors,
   [killswitches](killswitches) and [rate limits](rate_limits) are evaluated.
   Rejected requests never reach the handler.
4. **Metadata**: Another extractor constructs inbound `Metadata` from request
   headers. At this point, certain server-side fields are materialized, so they
   are provided consistently to the service and backends.
5. **Handler**: the endpoint handler calls the
   [`AuthAwareService`](auth::AuthAwareService), which checks permissions
   before delegating to the underlying
   [`StorageService`](objectstore_service::StorageService). The service
   enforces its own backpressure before executing the
   operation.
6. **Response**: metadata is mapped to HTTP headers (see
   [`objectstore-types` docs](objectstore_types) for the header mapping) and
   the payload is streamed back.

## Authentication & Authorization

Objectstore uses **JWT tokens with EdDSA signatures** (Ed25519) for
authentication. Auth enforcement is **enabled by default** and controlled by the
[`auth.enforce`](config) config flag. Set `enforce: false` explicitly for
unauthenticated development setups.

### Token Structure

Tokens must include:
- **Header**: `kid` (key ID) and `alg: EdDSA`
- **Claims**: `exp` (expiration timestamp)
- **Resource claims** (`res`): the usecase and scope values the token grants
  access to (e.g., `{"os:usecase": "attachments", "org": "123"}`)
- **Permissions**: array of granted operations (`object.read`, `object.write`,
  `object.delete`)

The token is supplied in the `x-os-auth` header (falling back to the standard
`Authorization` header), optionally prefixed with `Bearer `. It may also be
supplied as an `os_auth` query parameter, which lets callers embed a token
directly in a URL. The header takes precedence when both are present.

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

Its [`AuthContext`](auth::AuthContext) is one of `Disabled` (auth inactive — all
operations permitted), `Preauthorized` (a valid pre-signed URL already authorized
the exact read request), or `Scoped` (a verified JWT, checked per operation).

### Pre-signed URLs

Instead of a JWT, a request may authorize itself with a **pre-signed URL**: a
key holder signs a canonical form of the request with its Ed25519 key and encodes
the signature and parameters entirely in the query string (`os_sig`, `os_kid`,
`os_timestamp`, `os_duration`). See [`objectstore_types::presign`] for the
canonical form.

When the extractor sees an `os_sig` query parameter it takes the pre-signed
path instead of looking for a JWT:

- Only `GET` and `HEAD` are currently supported.
- The signature is verified against the request's canonical form using the
  `os_kid` key from the [`PublicKeyDirectory`](auth::PublicKeyDirectory).
- The signing key must have `ObjectRead` in its `max_permissions`.
- The validity window (`os_timestamp` + `os_duration`) is enforced, capped at
  **one week** so a URL cannot be minted to be effectively immortal.

A verified pre-signed request yields an `AuthContext::Preauthorized`. The
signature already binds the request's method, path, and parameters, so no scope
or permission check is needed at operation time — the key's read permission was
verified when the pre-signed URL was validated.

## Configuration

Configuration uses [figment](https://docs.rs/figment) for layered merging with
this precedence (highest wins):

1. **Environment variables** — prefixed with `OS__`, using `__` as a nested
   separator. Example: `OS__STORAGE__TYPE=tiered`
2. **YAML file** — passed via the `-c` / `--config` CLI flag
3. **Defaults** — sensible defaults (local filesystem backend,
   auth enabled)

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

Bandwidth limiting uses **debt-based GCRA** (Generic Cell Rate Algorithm)
buckets that track a theoretical arrival time. Payload streams are wrapped in a
`MeteredPayloadStream` that track consumed bytes. When accumulated debt exceeds
the configured threshold, new requests are rejected.

The `burst_ms` parameter controls how much transient overshoot is tolerated
before rejection (in milliseconds). Defaults to `1000` (1 second).

Like throughput, bandwidth limits can be set at multiple granularities:

- **Global**: a maximum bytes-per-second across all traffic (`global_bps`)
- **Per-usecase**: a percentage of the global limit for each usecase
- **Per-scope**: a percentage of the global limit for each scope value

Each granularity maintains its own bucket. The `MeteredPayloadStream` charges
all applicable buckets (global + per-usecase + per-scope) for every chunk
polled. For non-streamed payloads, bytes are recorded directly.

Rate-limited requests receive HTTP 429. When `report_only` is enabled, all
accounting and metrics remain active, but requests exceeding the limit are
admitted instead of rejected.

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

#### Gauges

| Resource | Utilization | Limit |
|---|---|---|
| HTTP concurrency | `objectstore_requests_in_flight` | `objectstore_requests_limit` |
| Task concurrency | `objectstore_tasks_running` | `objectstore_tasks_limit` |

Limit gauges are emitted only when the corresponding limit is configured.

#### Counters

Monotonically increasing totals since startup; use `irate(counter[window])` in
KEDA queries for an unsmoothed, immediately responsive rate:

| Counter | Description |
|---|---|
| `objectstore_bytes_total` | Total bytes transferred since startup |
| `objectstore_requests_total` | Total admitted requests since startup |

### Example KEDA ScaledObject Trigger

Scale on the highest utilization across all resources using `irate()` for
bandwidth/throughput rates:

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
- **Service**: a glob pattern matched against the normalized
  `x-downstream-service` value (Kubernetes hash/pod suffixes are stripped on
  ingest, so patterns match the base service name, e.g. `"relay*"`)

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
