# Shared Types

This crate defines the types shared between the objectstore server, service,
and client libraries. It is the common vocabulary that ensures all components
agree on how metadata is represented, how scopes work, what permissions exist,
and how objects expire.

## Metadata

[`Metadata`] is the per-object metadata structure carried alongside every
object. It travels through the entire system: clients set it via HTTP headers,
the server parses and validates it, the service passes it to backends, and
backends persist it.

### Fields

| Field                  | Type                        | Description                                    |
|------------------------|-----------------------------|------------------------------------------------|
| `is_redirect_tombstone`| `Option<bool>`              | Internal: marks redirect tombstones (see below) |
| `expiration_policy`    | [`ExpirationPolicy`]        | Automatic cleanup policy                       |
| `time_created`         | `Option<SystemTime>`        | Set by the server on each write                |
| `time_expires`         | `Option<SystemTime>`        | Resolved expiration timestamp                  |
| `content_type`         | `Cow<'static, str>`         | IANA media type (default: `application/octet-stream`) |
| `compression`          | `Option<Compression>`       | Compression algorithm, if any                  |
| `origin`               | `Option<String>`            | Source IP of the original uploader              |
| `size`                 | `Option<usize>`             | Object size in bytes                           |
| `custom`               | `BTreeMap<String, String>`  | User-provided key-value metadata               |

### HTTP Header Mapping

Metadata is transmitted as HTTP headers. Standard HTTP headers are used where
applicable; objectstore-specific fields use the `x-sn-*` prefix:

| Field              | HTTP Header              |
|--------------------|--------------------------|
| `content_type`     | `Content-Type`           |
| `compression`      | `Content-Encoding`       |
| `expiration_policy`| `x-sn-expiration`       |
| `time_created`     | `x-sn-time-created`     |
| `time_expires`     | `x-sn-time-expires`     |
| `origin`           | `x-sn-origin`           |
| `is_redirect_tombstone` | `x-sn-redirect-tombstone` |

Custom user metadata uses the `x-snme-` prefix — for example, a custom field
`"build_id"` becomes the header `x-snme-build_id`.

Backends that store metadata as object metadata (like GCS) add their own prefix
on top, so `x-sn-expiration` becomes `x-goog-meta-x-sn-expiration` in GCS.

### Redirect Tombstones

The [`is_redirect_tombstone`](Metadata::is_redirect_tombstone) field is an
internal mechanism, not set by clients. When the service layer stores a large
object in the long-term backend, it writes a tombstone (an empty object with
this field set to `true`) in the high-volume backend. Reads check this field to
know they should follow the redirect. See the `objectstore-service` crate
documentation for the full tombstone protocol.

**Note:** This field must remain the first field in the struct. The BigTable
backend uses a regex predicate on the serialized JSON that assumes
`is_redirect_tombstone` appears at the start.

## Scope System

The [`scope`] module defines the hierarchical namespace used to organize and
authorize access to objects.

### Scope

A [`Scope`](scope::Scope) is a single key-value pair like `organization=17` or
`project=42`. Both the key and value must be non-empty and contain only allowed
characters:

```text
A-Z a-z 0-9 _ - ( ) $ ! + '
```

Characters used as delimiters are forbidden: `.` (storage path separator),
`/` (path separator), `=` and `;` (API path encoding).

### Scopes

[`Scopes`](scope::Scopes) is an ordered collection of `Scope` values. Order
matters — `organization=17;project=42` and `project=42;organization=17`
identify different object namespaces because they produce different storage
paths.

Scopes serve two purposes:
1. **Organization**: they define a hierarchical folder-like structure within a
   usecase. The storage path `org.17/project.42/objects/{key}` directly reflects
   the scope hierarchy.
2. **Authorization**: JWT tokens include scope claims that are matched against
   the request's scopes. A token scoped to `organization=17` can only access
   objects under that organization.

Scopes have two display formats:
- **Storage path** (`as_storage_path`): `org.17/project.42` — used by backends
  to construct storage keys
- **API path** (`as_api_path`): `org=17;project=42` — used in HTTP URL paths
  (Matrix URI syntax). Empty scopes render as `_`.

## Expiration Policies

[`ExpirationPolicy`] controls automatic object cleanup:

| Variant        | Format       | Behavior                                      |
|----------------|--------------|-----------------------------------------------|
| `Manual`       | `manual`     | No automatic expiration (default)             |
| `TimeToLive`   | `ttl:30s`    | Expires after a fixed duration from creation  |
| `TimeToIdle`   | `tti:1h`     | Expires after a duration of no access         |

Durations use [humantime](https://docs.rs/humantime) format (e.g., `30s`,
`5m`, `1h`, `7d`). The policy is set by the client at upload time and persisted
with the object.

**Important:** `Manual` is the default and must remain so — persisted objects
without an explicit policy are deserialized as `Manual`.

## Compression

[`Compression`] indicates the compression algorithm applied to the object
payload. Currently only `Zstd` (Zstandard) is supported. Compression is
reflected in the `Content-Encoding` HTTP header.

## Permissions

[`Permission`] defines the three operation types that can be authorized:

| Variant        | Serialized as     | Grants                  |
|----------------|-------------------|-------------------------|
| `ObjectRead`   | `"object.read"`   | Read/download objects   |
| `ObjectWrite`  | `"object.write"`  | Create/overwrite objects |
| `ObjectDelete` | `"object.delete"` | Delete objects          |

Permissions are carried in JWT tokens and checked by the server's authorization
layer before each operation.
