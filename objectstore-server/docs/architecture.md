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

Health endpoints:
- `GET /health` — liveness probe (always returns 200)
- `GET /ready` — readiness probe (returns 503 when `/tmp/objectstore.down`
  exists, enabling graceful drain)

## Request Flow

A request flows through several layers before reaching the storage service:

1. **Middleware**: metrics collection, in-flight request tracking, panic
   recovery, Sentry transaction tracing, distributed tracing.
2. **Extractors**: path parameters are parsed into an
   [`ObjectId`](objectstore_service::id::ObjectId) or
   [`ObjectContext`](objectstore_service::id::ObjectContext). The `Authorization`
   header is validated and decoded into an [`AuthContext`](auth::AuthContext).
   The optional `x-downstream-service` header is extracted for killswitch
   matching.
3. **Admission control**: [killswitches](killswitches) and
   [rate limits](rate_limits) are checked during extraction. Rejected requests
   never reach the handler.
4. **Handler**: the endpoint handler calls the
   [`AuthAwareService`](auth::AuthAwareService), which checks permissions
   before delegating to the underlying
   [`StorageService`](objectstore_service::StorageService).
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
   separator. Example: `OS__LONG_TERM_STORAGE__TYPE=gcs`
2. **YAML file** — passed via the `-c` / `--config` CLI flag
3. **Defaults** — sensible development defaults (local filesystem backends,
   auth disabled)

Key configuration sections:
- `high_volume_storage` / `long_term_storage` — backend type and connection
  parameters
- `auth` — key directory and enforcement toggle
- `rate_limits` — throughput and bandwidth limits
- `killswitches` — traffic blocking rules
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
[`record_bandwidth`](ServiceState::record_bandwidth).

Rate-limited requests receive HTTP 429.

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
