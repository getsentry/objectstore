# Changelog

## 0.3.0

### Breaking Changes

- Remove the deprecated `Session::mint_token()` by @sentry-junior in [#717](https://github.com/getsentry/objectstore/pull/717)
  - Replace `session.mint_token()` with `session.get_token()`.
- Remove the deprecated `TokenGenerator::sign()` by @sentry-junior in [#716](https://github.com/getsentry/objectstore/pull/716)
  - Replace `generator.sign(&scope)` with `generator.create_token(&scope).sign()`.
- Remove the deprecated `PutBuilder::compression()` by @sentry-junior in [#715](https://github.com/getsentry/objectstore/pull/715)
  - Replace `.compression(...)` with `.compress(...)` on put builders.
- `Usecase::new` now returns a `Result`, and the panicking constructor is now `Usecase::from_static` by @sentry-junior in [#710](https://github.com/getsentry/objectstore/pull/710)
  - Replace `Usecase::new("name")` with `Usecase::from_static("name")`.
  - Replace `Usecase::try_new(name)` with `Usecase::new(name)`.
- Mark `Error` as `#[non_exhaustive]` so new error variants can be added without a breaking change by @lcian in [#706](https://github.com/getsentry/objectstore/pull/706)
  - Add a wildcard arm (`_ => ...`) to exhaustive `match` expressions on `objectstore_client::Error`.
- Remove the multipart upload API (`Session::initiate_multipart_upload()`, `Session::resume_multipart_upload()`) and the `multipart` feature flag by @lcian in [#704](https://github.com/getsentry/objectstore/pull/704)
  - Use resumable uploads instead.
  - Remove `match` arms for the removed `Error::InvalidPartNumber`, `Error::InvalidUploadId` and `Error::MultipartComplete` variants.

### New Features

- Support gzip compression in server and clients by @sentry-junior in [#693](https://github.com/getsentry/objectstore/pull/693)

## 0.2.10

### Bug Fixes

- Validate usecase names by @jan-auer in [#694](https://github.com/getsentry/objectstore/pull/694)

### Other Changes

- Enable resumable uploads on docs.rs by @jan-auer in [#684](https://github.com/getsentry/objectstore/pull/684)
- Introduce a validated usecase name type by @jan-auer in [#695](https://github.com/getsentry/objectstore/pull/695)

## 0.2.9

### New Features

- Surface upload offset mismatch as an error by @lcian in [#654](https://github.com/getsentry/objectstore/pull/654)

## 0.2.8

### New Features

- Define and expose upload granularity by @lcian in [#652](https://github.com/getsentry/objectstore/pull/652)

## 0.2.7

### New Features

- Add lifetime extensions for Python and Rust by @jan-auer in [#639](https://github.com/getsentry/objectstore/pull/639)
- Support resumable uploads by @lcian in [#640](https://github.com/getsentry/objectstore/pull/640)
- Add streaming versions of `put_chunk` by @lcian in [#649](https://github.com/getsentry/objectstore/pull/649)

### Bug Fixes

- Update TTL durations on expiry extensions by @jan-auer in [#646](https://github.com/getsentry/objectstore/pull/646)

## 0.2.6

### New Features

- Add Resumable Uploads (low-level) API by @lcian in [#622](https://github.com/getsentry/objectstore/pull/622)

### Bug Fixes

- Drain discarded response bodies by @lcian in [#629](https://github.com/getsentry/objectstore/pull/629)

### Other Changes

- Use typed queries for resumable API by @lcian in [#628](https://github.com/getsentry/objectstore/pull/628)

## 0.2.5

- No documented changes.

## 0.2.4

- No documented changes.

## 0.2.3

### New Features

- Mint scoped tokens through a TokenRequest builder by @matt-codecov in [#545](https://github.com/getsentry/objectstore/pull/545)

### Bug Fixes

- Restrict the duration wire format to days and smaller by @jan-auer in [#592](https://github.com/getsentry/objectstore/pull/592)

## 0.2.2

### New Features

- Allow marking uploads as precompressed by @jan-auer in [#586](https://github.com/getsentry/objectstore/pull/586)

## 0.2.1

### New Features

- Support Unicode in object metadata headers by @jan-auer in [#581](https://github.com/getsentry/objectstore/pull/581)

## 0.2.0

- No documented changes.

## 0.1.15

- No documented changes.

## 0.1.14

- No documented changes.

## 0.1.13

### New Features

- Accept optional tokens in ClientBuilder::token by @jan-auer in [#516](https://github.com/getsentry/objectstore/pull/516)
- Add filename field to Metadata by @lcian in [#517](https://github.com/getsentry/objectstore/pull/517)

### Bug Fixes

- Apply pedantic Clippy lints across workspace by @jan-auer in [#513](https://github.com/getsentry/objectstore/pull/513)

### Other Changes

- Fix and expand documentation across the workspace by @jan-auer in [#521](https://github.com/getsentry/objectstore/pull/521)
- Fix multipart endpoint paths and client examples by @jan-auer in [#505](https://github.com/getsentry/objectstore/pull/505)
- Remove version pin notice on tokio dependency by @jan-auer in [#531](https://github.com/getsentry/objectstore/pull/531)
- Replace test key LazyLock statics with compile-time constants by @jan-auer in [#510](https://github.com/getsentry/objectstore/pull/510)
- Consolidate workspace dependencies and pin explicit versions by @jan-auer in [#507](https://github.com/getsentry/objectstore/pull/507)

## 0.1.12

### Bug Fixes

- Classify zstd batch inserts by worst-case post-compression size by @lcian in [#500](https://github.com/getsentry/objectstore/pull/500)

## 0.1.11

- No documented changes.

## 0.1.10

### New Features

- Add Multipart Upload API by @lcian in [#468](https://github.com/getsentry/objectstore/pull/468)

## 0.1.9

### New Features

- Make concurrency configurable in ManyBuilder by @lcian in [#457](https://github.com/getsentry/objectstore/pull/457)
- Add batchable HEAD operation by @lcian in [#478](https://github.com/getsentry/objectstore/pull/478)

## 0.1.8

- No documented changes.

## 0.1.7

### New Features

- Default auth enforcement to true by @lcian in [#447](https://github.com/getsentry/objectstore/pull/447)

## 0.1.6

### New Features

- Add X-Os-Auth as alternative auth header by @lcian in [#422](https://github.com/getsentry/objectstore/pull/422)

## 0.1.5

### New Features

- Add mint_token() method to Session API by @matt-codecov in [#416](https://github.com/getsentry/objectstore/pull/416)

## 0.1.4

### New Features

- Add 100 MB per-batch body size limit by @jan-auer in [#417](https://github.com/getsentry/objectstore/pull/417)

## 0.1.3

### New Features

- Add put_path for lazy file descriptor opening by @jan-auer in [#415](https://github.com/getsentry/objectstore/pull/415)

### Other Changes

- Refactor many API into combinator pipeline by @lcian in [#391](https://github.com/getsentry/objectstore/pull/391)

## 0.1.2

### New Features

- Add file variant with more reliable batching by @lcian in [#346](https://github.com/getsentry/objectstore/pull/346)

## 0.1.1

- No documented changes.

## 0.1.0

### New Features

- Add accept_encoding to Rust and Python get methods by @jan-auer in [#344](https://github.com/getsentry/objectstore/pull/344)

## 0.0.19

### New Features

- Implement many API using batch endpoint by @lcian in [#277](https://github.com/getsentry/objectstore/pull/277)

## 0.0.18

### New Features

- Add static token auth, consolidate token generator by @lcian in [#330](https://github.com/getsentry/objectstore/pull/330)

### Bug Fixes

- Check HTTP status on DELETE responses by @lcian in [#333](https://github.com/getsentry/objectstore/pull/333)

## 0.0.17

### New Features

- Add origin as built-in metadata field by @jan-auer in [#292](https://github.com/getsentry/objectstore/pull/292)

### Other Changes

- Restructure Rust and Python client READMEs by @jan-auer in [#294](https://github.com/getsentry/objectstore/pull/294)
- Add crate-level architecture documentation by @jan-auer in [#304](https://github.com/getsentry/objectstore/pull/304)
- Extract modules and distribute docs by @jan-auer in [#307](https://github.com/getsentry/objectstore/pull/307)
- Move tombstone + custom-time out of Metadata header methods by @jan-auer in [#303](https://github.com/getsentry/objectstore/pull/303)

## 0.0.16

### Bug Fixes

- Re-export objectstore_types::Permission as part of API by @matt-codecov in [#256](https://github.com/getsentry/objectstore/pull/256)

### Other Changes

- Bump reqwest to 0.13.1 by @lcian in [#282](https://github.com/getsentry/objectstore/pull/282)
- Improve assertion by @lcian in [#280](https://github.com/getsentry/objectstore/pull/280)

## 0.0.15

### New Features

- Rust client support for bearer auth by @matt-codecov in [#237](https://github.com/getsentry/objectstore/pull/237)

### Other Changes

- Change defaults for timeouts by @jan-auer in [#246](https://github.com/getsentry/objectstore/pull/246)
- Move `Permission`, `Scope`, `Scopes` to `objectstore_types` and use in Rust client by @matt-codecov in [#236](https://github.com/getsentry/objectstore/pull/236)
- Enable authorization tokens in e2e client tests by @matt-codecov in [#231](https://github.com/getsentry/objectstore/pull/231)
- Add tests for keys containing slashes by @jan-auer in [#245](https://github.com/getsentry/objectstore/pull/245)

## 0.0.14

### New Features

- Implement the new Web API by @jan-auer in [#230](https://github.com/getsentry/objectstore/pull/230)

## 0.0.13

- No documented changes.

## 0.0.12

### New Features

- Add time_creation as first-class metadata by @lcian in [#210](https://github.com/getsentry/objectstore/pull/210)
- Append to the given base URL path by @lcian in [#222](https://github.com/getsentry/objectstore/pull/222)

## 0.0.11

### Breaking Changes

- Change url format: {usecase}/{scope1}/.../data/{key} by @matt-codecov in [#203](https://github.com/getsentry/objectstore/pull/203)

## 0.0.10

### New Features

- Add a util to guess MIME type by @lcian in [#200](https://github.com/getsentry/objectstore/pull/200)

### Other Changes

- Add some more docs for new Rust API by @lcian in [#194](https://github.com/getsentry/objectstore/pull/194)

## 0.0.9

### New Features

- Introduce new API by @lcian in [#190](https://github.com/getsentry/objectstore/pull/190)

## 0.0.8

- No documented changes.

## 0.0.7

### New Features

- Add missing PUT `key` to the Rust client, replace `anyhow` with… by @Swatinem in [#191](https://github.com/getsentry/objectstore/pull/191)

## 0.0.6

- No documented changes.

## 0.0.5

- No documented changes.

## 0.0.4

- No documented changes.

## 0.0.3

- No documented changes.

## 0.0.1

This is the first release of the Objectstore client SDKs.
