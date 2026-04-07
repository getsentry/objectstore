# Plan: Integrate Killswitches with objectstore-options

## Goal

Replace the static config-backed `Killswitches` with a live-reloading implementation
backed by `objectstore_options`, while keeping compiled glob matchers cached across
option refresh cycles.

## Changes

### 1. Move `Killswitch` to `objectstore-options`

`objectstore_options::Killswitch` already exists as the plain data type. The
`objectstore_server::Killswitch` struct (with `service_matcher: OnceLock<Option<GlobMatcher>>`)
is removed entirely. All construction sites currently using the server type switch to
`objectstore_options::Killswitch`.

### 2. Refactor `Killswitches` in `objectstore-server`

- Make fields private; add a `Killswitches::new()` constructor.
- Remove `Deserialize`/`Serialize` derives (no longer loaded from YAML config).
- Remove the `From<objectstore_options::Killswitch>` impl (no longer needed).
- Remove `PartialEq` impl (no longer needed without `service_matcher` to skip).

### 3. Introduce a global LRU matcher cache

In `killswitches.rs`, add:

```rust
static MATCHER_CACHE: Mutex<LruCache<String, GlobMatcher>> = ...;
const MATCHER_CACHE_SIZE: usize = 256;
```

Add `lru` as a dependency in `objectstore-server/Cargo.toml`.

### 4. Move matching logic from `Killswitch` to `Killswitches`

- Remove `impl Killswitch { fn matches(...) }`.
- Add a private `fn match_one(k: &objectstore_options::Killswitch, ctx, service) -> bool`
  on `Killswitches` (or as a free function in the module) that uses `MATCHER_CACHE`.
- `Killswitches::matches` and `Killswitches::find` call `match_one` per entry.

### 5. Wire up `Killswitches::matches` to live options

`Killswitches::matches` calls `objectstore_options::Options::get()` on every invocation
to get the fresh killswitch list, rather than iterating stored entries.

> This implies `Killswitches` itself becomes a zero-field unit struct (or is removed
> in favour of free functions). To be decided.

### 6. Remove `killswitches` field from `Config`

Once killswitches are driven by options, remove `pub killswitches: Killswitches` from
`Config` and its `default()` impl. Update all call sites that construct `Config` with
explicit killswitches (tests in `config.rs`, `extractors/id.rs`, `tests/limits.rs`) to
use `sentry_options::testing::set_override` instead.

## Affected Files

- `objectstore-options/src/lib.rs` — `Killswitch` type already present, no changes needed
- `objectstore-server/src/killswitches.rs` — main rework
- `objectstore-server/src/config.rs` — remove field, update tests
- `objectstore-server/src/extractors/id.rs` — update tests
- `objectstore-server/tests/limits.rs` — update integration test
- `objectstore-server/Cargo.toml` — add `lru` dependency

## Open Questions

- Does `Killswitches` become a unit struct (just a namespace for the `matches` function),
  or do we keep it as a newtype for the list? If options are always fetched live, there
  is nothing to store.
- The `limits.rs` integration test constructs a full `TestServer` with specific
  killswitches. With config-backed killswitches gone, this test needs to set options
  overrides and ensure `init` has been called or a test shim is in place.
