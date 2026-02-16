# Changelog

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
