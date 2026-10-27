# Changelog

## 0.3.0

### Breaking Changes

- Split the signing key from the token policy: `SecretKey(kid, secret_key)` now only holds the EdDSA key, and `TokenGenerator(key, expiry_seconds=60, permissions=None)` wraps it with the token defaults and is what you pass to `Client(token=...)`. The deprecated `sign_for_scope()` alias is removed. by @sentry-junior in [#721](https://github.com/getsentry/objectstore/pull/721)
  - Replace `SecretKey(kid, secret_key, expiry_seconds, permissions)` or `TokenGenerator(kid, secret_key, ...)` with `TokenGenerator(SecretKey(kid, secret_key), expiry_seconds, permissions)`.
    ```python
    # Before
    client = Client(url, token=SecretKey(kid, secret_key, expiry_seconds=300))
    
    # After
    key = SecretKey(kid, secret_key)
    client = Client(url, token=TokenGenerator(key, expiry_seconds=300))
    ```
  - Replace `.sign_for_scope(...)` with `.token_for_scope(...)`.
- Remove the deprecated `compression` argument from `Session.put()` by @sentry-junior in [#718](https://github.com/getsentry/objectstore/pull/718)
  - Replace `session.put(..., compression=...)` with `session.put(..., compress=...)`.
- Remove the deprecated `propagate_traces` argument from `Client` by @sentry-junior in [#714](https://github.com/getsentry/objectstore/pull/714)
  - Drop `propagate_traces=...` from `Client(...)` calls. Sentry's `StdlibIntegration`, enabled by default, already propagates traces on Objectstore requests.
- Remove the multipart upload API (`Session::initiate_multipart_upload()`, `Session::resume_multipart_upload()`) and the `multipart` feature flag by @lcian in [#704](https://github.com/getsentry/objectstore/pull/704)
  - Use resumable uploads instead.
  - Remove `match` arms for the removed `Error::InvalidPartNumber`, `Error::InvalidUploadId` and `Error::MultipartComplete` variants.

### New Features

- Add a `response_content_disposition` option (`"inline"` or `"attachment"`) to `Session.object_url()` and `Session.presigned_object_url()`. Pre-signed URLs include it in their signature. by @jan-auer in [#696](https://github.com/getsentry/objectstore/pull/696)
- Support gzip compression in server and clients by @sentry-junior in [#693](https://github.com/getsentry/objectstore/pull/693)

## 0.2.10

### New Features

- Add automatic resumable uploads by @lcian in [#667](https://github.com/getsentry/objectstore/pull/667)
- Add private resumable upload API by @lcian in [#665](https://github.com/getsentry/objectstore/pull/665)

### Bug Fixes

- Honor zero retries by @lcian in [#679](https://github.com/getsentry/objectstore/pull/679)
- Validate usecase names by @jan-auer in [#694](https://github.com/getsentry/objectstore/pull/694)

## 0.2.9

- No documented changes.

## 0.2.8

- No documented changes.

## 0.2.7

### New Features

- Add lifetime extensions for Python and Rust by @jan-auer in [#639](https://github.com/getsentry/objectstore/pull/639)

### Bug Fixes

- Update TTL durations on expiry extensions by @jan-auer in [#646](https://github.com/getsentry/objectstore/pull/646)

## 0.2.6

- No documented changes.

## 0.2.5

### New Features

- Add Sentry span instrumentation for storage operations by @jan-auer in [#602](https://github.com/getsentry/objectstore/pull/602)

## 0.2.4

### New Features

- Deprecate `propagate_traces` by @jan-auer in [#603](https://github.com/getsentry/objectstore/pull/603)
- Send a default User-Agent header by @jan-auer in [#601](https://github.com/getsentry/objectstore/pull/601)

## 0.2.3

### Bug Fixes

- Restrict the duration wire format to days and smaller by @jan-auer in [#592](https://github.com/getsentry/objectstore/pull/592)

## 0.2.2

### New Features

- Allow marking uploads as precompressed by @jan-auer in [#586](https://github.com/getsentry/objectstore/pull/586)

## 0.2.1

### New Features

- Support Unicode in object metadata headers by @jan-auer in [#581](https://github.com/getsentry/objectstore/pull/581)

### Bug Fixes

- Return 200 instead of 204 for object HEAD requests by @jan-auer in [#577](https://github.com/getsentry/objectstore/pull/577)

## 0.2.0

### Bug Fixes

- Return None from get() on 404 to match Rust client by @jan-auer in [#570](https://github.com/getsentry/objectstore/pull/570)
- Align empty-scope URL rendering with Rust client by @jan-auer in [#568](https://github.com/getsentry/objectstore/pull/568)
- Use pool scheme and bracket IPv6 hosts in object URLs by @jan-auer in [#567](https://github.com/getsentry/objectstore/pull/567)

## 0.1.15

### New Features

- Read JWT from query params by @lcian in [#556](https://github.com/getsentry/objectstore/pull/556)

### Other Changes

- Fix scope character set in Client.session docstring by @jan-auer in [#551](https://github.com/getsentry/objectstore/pull/551)
- Mark pre-signed URLs as experimental by @lcian in [#557](https://github.com/getsentry/objectstore/pull/557)

## 0.1.14

### New Features

- Support creating pre-signed URLs by @lcian in [#544](https://github.com/getsentry/objectstore/pull/544)

### Bug Fixes

- Treat object keys as literal strings by @lcian in [#548](https://github.com/getsentry/objectstore/pull/548)

## 0.1.13

### New Features

- Add filename field to Metadata by @lcian in [#517](https://github.com/getsentry/objectstore/pull/517)

### Other Changes

- Fix and expand documentation across the workspace by @jan-auer in [#521](https://github.com/getsentry/objectstore/pull/521)
- Fix multipart endpoint paths and client examples by @jan-auer in [#505](https://github.com/getsentry/objectstore/pull/505)

## 0.1.12

- No documented changes.

## 0.1.11

### New Features

- Add HEAD API by @lcian in [#488](https://github.com/getsentry/objectstore/pull/488)

## 0.1.10

### New Features

- Add Multipart Upload API by @lcian in [#476](https://github.com/getsentry/objectstore/pull/476)

## 0.1.9

- No documented changes.

## 0.1.8

### New Features

- Pin `uv_build==0.9.26` by @lcian in [#460](https://github.com/getsentry/objectstore/pull/460)

### Bug Fixes

- Unpin `uv_build` by @lcian in [#459](https://github.com/getsentry/objectstore/pull/459)

## 0.1.7

### New Features

- Add permission and expiry overrides to sign_for_scope by @lcian in [#432](https://github.com/getsentry/objectstore/pull/432)

## 0.1.6

### New Features

- Add X-Os-Auth as alternative auth header by @lcian in [#422](https://github.com/getsentry/objectstore/pull/422)

### Other Changes

- Pin uv_build version with upper bound by @jan-auer in [#420](https://github.com/getsentry/objectstore/pull/420)

## 0.1.5

### New Features

- Add mint_token() method to Session API by @matt-codecov in [#416](https://github.com/getsentry/objectstore/pull/416)

## 0.1.4

- No documented changes.

## 0.1.3

### Other Changes

- Collapse dual storage fields into a single StorageConfig by @jan-auer in [#384](https://github.com/getsentry/objectstore/pull/384)

## 0.1.2

- No documented changes.

## 0.1.1

### New Features

- Add parse_accept_encoding and fix wildcard handling by @jan-auer in [#347](https://github.com/getsentry/objectstore/pull/347)

## 0.1.0

### New Features

- Add accept_encoding to Rust and Python get methods by @jan-auer in [#344](https://github.com/getsentry/objectstore/pull/344)

## 0.0.19

- No documented changes.

## 0.0.18

### New Features

- Add static token auth, consolidate token generator by @lcian in [#330](https://github.com/getsentry/objectstore/pull/330)
- Improve retries for compressed bodies by @lcian in [#329](https://github.com/getsentry/objectstore/pull/329)

### Bug Fixes

- Avoid mutable default argument in TokenGenerator by @lcian in [#336](https://github.com/getsentry/objectstore/pull/336)

## 0.0.17

### New Features

- Add origin as built-in metadata field by @jan-auer in [#292](https://github.com/getsentry/objectstore/pull/292)

### Other Changes

- Restructure Rust and Python client READMEs by @jan-auer in [#294](https://github.com/getsentry/objectstore/pull/294)
- Add crate-level architecture documentation by @jan-auer in [#304](https://github.com/getsentry/objectstore/pull/304)

## 0.0.16

### New Features

- Move to structured ApiError by @lcian in [#263](https://github.com/getsentry/objectstore/pull/263)

### Other Changes

- Use internal PyPI exclusively by @lcian in [#272](https://github.com/getsentry/objectstore/pull/272)

## 0.0.15

### New Features

- Python client support for bearer auth by @matt-codecov in [#240](https://github.com/getsentry/objectstore/pull/240)

### Bug Fixes

- Set timeout correctly and update test by @jan-auer in [#242](https://github.com/getsentry/objectstore/pull/242)

### Other Changes

- Change defaults for timeouts by @jan-auer in [#246](https://github.com/getsentry/objectstore/pull/246)
- Fix ci and add python client comment about token_generator by @matt-codecov in [#252](https://github.com/getsentry/objectstore/pull/252)
- Enable authorization tokens in e2e client tests by @matt-codecov in [#231](https://github.com/getsentry/objectstore/pull/231)
- Add tests for keys containing slashes by @jan-auer in [#245](https://github.com/getsentry/objectstore/pull/245)

## 0.0.14

### New Features

- Implement the new Web API by @jan-auer in [#230](https://github.com/getsentry/objectstore/pull/230)

## 0.0.13

### Bug Fixes

- Don't call str on bytes by @lcian in [#225](https://github.com/getsentry/objectstore/pull/225)

## 0.0.12

### New Features

- Add time_creation as first-class metadata by @lcian in [#210](https://github.com/getsentry/objectstore/pull/210)
- Append to the given base URL path by @lcian in [#222](https://github.com/getsentry/objectstore/pull/222)
- Add time_expires as first-class metadata by @lcian in [#217](https://github.com/getsentry/objectstore/pull/217)

## 0.0.11

### Breaking Changes

- Change url format: {usecase}/{scope1}/.../data/{key} by @matt-codecov in [#203](https://github.com/getsentry/objectstore/pull/203)

## 0.0.10

### New Features

- Add a util to guess MIME type by @lcian in [#197](https://github.com/getsentry/objectstore/pull/197)

### Other Changes

- Rename a couple things by @lcian in [#196](https://github.com/getsentry/objectstore/pull/196)

## 0.0.9

- No documented changes.

## 0.0.8

### New Features

- Introduce new API by @lcian in [#184](https://github.com/getsentry/objectstore/pull/184)
- Implement "redirect tombstones" by @Swatinem in [#186](https://github.com/getsentry/objectstore/pull/186)

### Other Changes

- Use `OS__` as env prefix for config overrides by @jan-auer in [#192](https://github.com/getsentry/objectstore/pull/192)

## 0.0.7

- No documented changes.

## 0.0.6

### Other Changes

- Introduce commands and add healthcheck by @jan-auer in [#185](https://github.com/getsentry/objectstore/pull/185)
- Set up combined page for API docs by @lcian in [#178](https://github.com/getsentry/objectstore/pull/178)

## 0.0.5

### Other Changes

- Downgrade urllib3 requirement by @lcian in [#179](https://github.com/getsentry/objectstore/pull/179)

## 0.0.4

### Other Changes

- Make sure we actually test the python timeouts by @Swatinem in [#175](https://github.com/getsentry/objectstore/pull/175)
- Downgrade to Python 3.11 for our internal PyPI by @Swatinem in [#177](https://github.com/getsentry/objectstore/pull/177)

## 0.0.3

- No documented changes.

## 0.0.1

This is the first release of the Objectstore client SDKs.
