# Changelog

## 0.1.14

### New Features ✨

- (py-client) Support creating pre-signed URLs by @lcian in [#544](https://github.com/getsentry/objectstore/pull/544)

### Bug Fixes 🐛

- (py-client) Treat object keys as literal strings by @lcian in [#548](https://github.com/getsentry/objectstore/pull/548)

## 0.1.13

### New Features ✨

- (client) Accept optional tokens in ClientBuilder::token by @jan-auer in [#516](https://github.com/getsentry/objectstore/pull/516)
- Add filename field to Metadata by @lcian in [#517](https://github.com/getsentry/objectstore/pull/517)

### Bug Fixes 🐛

- Apply pedantic Clippy lints across workspace by @jan-auer in [#513](https://github.com/getsentry/objectstore/pull/513)

### Documentation 📚

- Fix and expand documentation across the workspace by @jan-auer in [#521](https://github.com/getsentry/objectstore/pull/521)
- Fix multipart endpoint paths and client examples by @jan-auer in [#505](https://github.com/getsentry/objectstore/pull/505)

### Internal Changes 🔧

- (client) Remove version pin notice on tokio dependency by @jan-auer in [#531](https://github.com/getsentry/objectstore/pull/531)
- Replace test key LazyLock statics with compile-time constants by @jan-auer in [#510](https://github.com/getsentry/objectstore/pull/510)
- Consolidate workspace dependencies and pin explicit versions by @jan-auer in [#507](https://github.com/getsentry/objectstore/pull/507)

## 0.1.12

### Bug Fixes 🐛

- (rust-client) Classify zstd batch inserts by worst-case post-compression size by @lcian in [#500](https://github.com/getsentry/objectstore/pull/500)

## 0.1.11

### New Features ✨

- (py-client) Add HEAD API by @lcian in [#488](https://github.com/getsentry/objectstore/pull/488)

## 0.1.10

### New Features ✨

- (py-client) Add Multipart Upload API by @lcian in [#476](https://github.com/getsentry/objectstore/pull/476)
- (rust-client) Add Multipart Upload API by @lcian in [#468](https://github.com/getsentry/objectstore/pull/468)

## 0.1.9

### New Features ✨

- (rust-client) Make concurrency configurable in ManyBuilder by @lcian in [#457](https://github.com/getsentry/objectstore/pull/457)
- (server/rust-client) Add batchable HEAD operation by @lcian in [#478](https://github.com/getsentry/objectstore/pull/478)

## 0.1.8

### New Features ✨

- (py-client) Pin `uv_build==0.9.26` by @lcian in [#460](https://github.com/getsentry/objectstore/pull/460)

### Bug Fixes 🐛

- (py-client) Unpin `uv_build` by @lcian in [#459](https://github.com/getsentry/objectstore/pull/459)

## 0.1.7

### New Features ✨

- (auth) Default auth enforcement to true by @lcian in [#447](https://github.com/getsentry/objectstore/pull/447)
- (py-client) Add permission and expiry overrides to sign_for_scope by @lcian in [#432](https://github.com/getsentry/objectstore/pull/432)

## 0.1.6

### New Features ✨

- (server) Add X-Os-Auth as alternative auth header by @lcian in [#422](https://github.com/getsentry/objectstore/pull/422)

### Internal Changes 🔧

- (python) Pin uv_build version with upper bound by @jan-auer in [#420](https://github.com/getsentry/objectstore/pull/420)

## 0.1.5

### New Features ✨

- (client) Add mint_token() method to Session API by @matt-codecov in [#416](https://github.com/getsentry/objectstore/pull/416)

## 0.1.4

### New Features ✨

- (client) Add 100 MB per-batch body size limit by @jan-auer in [#417](https://github.com/getsentry/objectstore/pull/417)

## 0.1.3

### New Features ✨

- (client) Add put_path for lazy file descriptor opening by @jan-auer in [#415](https://github.com/getsentry/objectstore/pull/415)

### Internal Changes 🔧

- (client) Refactor many API into combinator pipeline by @lcian in [#391](https://github.com/getsentry/objectstore/pull/391)
- (config) Collapse dual storage fields into a single StorageConfig by @jan-auer in [#384](https://github.com/getsentry/objectstore/pull/384)

## 0.1.2

### New Features ✨

- (rust-client) Add file variant with more reliable batching by @lcian in [#346](https://github.com/getsentry/objectstore/pull/346)

## 0.1.1

### New Features ✨

- (clients/python) Add parse_accept_encoding and fix wildcard handling by @jan-auer in [#347](https://github.com/getsentry/objectstore/pull/347)

### Internal Changes 🔧

- Separate release-library branch prefix for client releases by @jan-auer in [#345](https://github.com/getsentry/objectstore/pull/345)

## 0.1.0

### New Features ✨

- (clients) Add accept_encoding to Rust and Python get methods by @jan-auer in [#344](https://github.com/getsentry/objectstore/pull/344)

## 0.0.19

### New Features ✨

- (rust-client) Implement many API using batch endpoint by @lcian in [#277](https://github.com/getsentry/objectstore/pull/277)

## 0.0.18

### New Features ✨

- (clients) Add static token auth, consolidate token generator by @lcian in [#330](https://github.com/getsentry/objectstore/pull/330)
- (python-client) Improve retries for compressed bodies by @lcian in [#329](https://github.com/getsentry/objectstore/pull/329)

### Bug Fixes 🐛

- (python-client) Avoid mutable default argument in TokenGenerator by @lcian in [#336](https://github.com/getsentry/objectstore/pull/336)
- (rust-client) Check HTTP status on DELETE responses by @lcian in [#333](https://github.com/getsentry/objectstore/pull/333)

## 0.0.17

### New Features ✨

- (types) Add origin as built-in metadata field by @jan-auer in [#292](https://github.com/getsentry/objectstore/pull/292)

### Documentation 📚

- (clients) Restructure Rust and Python client READMEs by @jan-auer in [#294](https://github.com/getsentry/objectstore/pull/294)
- Add crate-level architecture documentation by @jan-auer in [#304](https://github.com/getsentry/objectstore/pull/304)

### Internal Changes 🔧

#### Types

- Extract modules and distribute docs by @jan-auer in [#307](https://github.com/getsentry/objectstore/pull/307)
- Move tombstone + custom-time out of Metadata header methods by @jan-auer in [#303](https://github.com/getsentry/objectstore/pull/303)

## 0.0.16

### New Features ✨

- (server) Move to structured ApiError by @lcian in [#263](https://github.com/getsentry/objectstore/pull/263)

### Bug Fixes 🐛

- (rust-client) Re-export objectstore_types::Permission as part of API by @matt-codecov in [#256](https://github.com/getsentry/objectstore/pull/256)

### Internal Changes 🔧

- (rust-client) Bump reqwest to 0.13.1 by @lcian in [#282](https://github.com/getsentry/objectstore/pull/282)
- Use internal PyPI exclusively by @lcian in [#272](https://github.com/getsentry/objectstore/pull/272)

### Other

- test(rust-client): Improve assertion by @lcian in [#280](https://github.com/getsentry/objectstore/pull/280)

## 0.0.15

### New Features ✨

#### Auth

- Python client support for bearer auth by @matt-codecov in [#240](https://github.com/getsentry/objectstore/pull/240)
- Rust client support for bearer auth by @matt-codecov in [#237](https://github.com/getsentry/objectstore/pull/237)

### Bug Fixes 🐛

- (python) Set timeout correctly and update test by @jan-auer in [#242](https://github.com/getsentry/objectstore/pull/242)

### Build / dependencies / internal 🔧

- (clients) Change defaults for timeouts by @jan-auer in [#246](https://github.com/getsentry/objectstore/pull/246)
- Fix ci and add python client comment about token_generator by @matt-codecov in [#252](https://github.com/getsentry/objectstore/pull/252)
- Move `Permission`, `Scope`, `Scopes` to `objectstore_types` and use in Rust client by @matt-codecov in [#236](https://github.com/getsentry/objectstore/pull/236)

### Other

- tests(client): enable authorization tokens in e2e client tests by @matt-codecov in [#231](https://github.com/getsentry/objectstore/pull/231)
- test(client): Add tests for keys containing slashes by @jan-auer in [#245](https://github.com/getsentry/objectstore/pull/245)

## 0.0.14

### New Features ✨

- feat(server): Implement the new Web API by @jan-auer in [#230](https://github.com/getsentry/objectstore/pull/230)

## 0.0.13

### Fixes

- fix(py-client): Don't call str on bytes (#225) by @lcian

## 0.0.12

### Features

- feat: Add time_creation as first-class metadata (#210) by @lcian
- feat(clients): Append to the given base URL path (#222) by @lcian
- feat: Add time_expires as first-class metadata (#217) by @lcian

## 0.0.11

### Various fixes & improvements

- breaking: change url format: {usecase}/{scope1}/.../data/{key} (#203) by @matt-codecov

## 0.0.10

### Various fixes & improvements

- feat(rust-client): Add a util to guess MIME type (#200) by @lcian
- feat(py-client): Add a util to guess MIME type (#197) by @lcian
- docs(rust-client): Add some more docs for new Rust API (#194) by @lcian
- ref(py-client): Rename a couple things (#196) by @lcian

## 0.0.9

### Various fixes & improvements

- feat(rust-client): Introduce new API (#190) by @lcian

## 0.0.8

### Various fixes & improvements

- feat(py-client): Introduce new API (#184) by @lcian
- feat: Implement "redirect tombstones" (#186) by @Swatinem
- ref(server): Use `OS__` as env prefix for config overrides (#192) by @jan-auer

## 0.0.7

### Various fixes & improvements

- feat: Add missing PUT `key` to the Rust client, replace `anyhow` with… (#191) by @Swatinem

## 0.0.6

### Various fixes & improvements

- ref(cli): Introduce commands and add healthcheck (#185) by @jan-auer
- docs: Set up combined page for API docs (#178) by @lcian

## 0.0.5

### Various fixes & improvements

- build(py-client): Downgrade urllib3 requirement (#179) by @lcian

## 0.0.4

### Various fixes & improvements

- test: Make sure we actually test the python timeouts (#175) by @Swatinem
- chore: Downgrade to Python 3.11 for our internal PyPI (#177) by @Swatinem

## 0.0.3

- No documented changes.

## 0.0.1

This is the first release of the Objectstore client SDKs.
