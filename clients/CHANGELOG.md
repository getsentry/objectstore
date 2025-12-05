# Changelog

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
