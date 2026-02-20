# Claude Code Instructions for Objectstore

This file contains project-specific guidance for Claude Code when working on the Objectstore project.

## Running Tests

### Standard Test Command

When running tests, use the full workspace test command to ensure all packages and features are tested:

```bash
cargo test --workspace --all-features
```

**Important**: Do not use just `cargo test` as it may miss workspace members and feature-gated code.

### Backend Service Requirements

Some tests require external services (GCS emulator, Bigtable emulator) managed by `devservices`.

**Symptoms of missing services:**
- Connection refused errors
- TCP connect error messages
- `RPC error: status: Unavailable`
- Tests in `objectstore-service` for GCS/Bigtable backends fail

**How to fix:**

1. Check devservices status:
   ```bash
   devservices status
   ```

2. Start devservices if not running:
   ```bash
   devservices up --mode=full
   ```

3. Devservices run in the background - you only need to start them once per session

### Testing Individual Packages

To test only one package (e.g., when backend services aren't needed):

```bash
cargo test -p objectstore-server --all-features
```

## Formatting and Linting

After completing a batch of edits (before responding to the user), always run formatting and linting to keep the code clean. Do not defer this to commit time — run it every time you're done editing and about to hand control back.

### Rust

After editing Rust files, run formatting and clippy:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features --no-deps
```

Fix any issues before responding.

### Python

After editing Python files, run formatting, linting, and type checking:

```bash
uv run ruff format
uv run ruff check
uv run mypy .
```

Fix any issues before responding.

### Documentation Validation

When adding docs or moving types, verify documentation references:

```bash
cargo doc --workspace --all-features --no-deps --document-private-items
```

## After Every Iteration

Do these checks after completing changes and before responding to the user.

**Update architecture docs** if your changes affect documented behavior. Grep `docs/` for terms related to your change. The doc locations are:
- `objectstore-service/docs/architecture.md` — backends, tombstones, object identification, streaming
- `objectstore-server/docs/architecture.md` — endpoints, request flow, auth, config, rate limiting, killswitches
- `objectstore-types/src/lib.rs` + module docs — metadata, scopes, expiration, compression, permissions

**Reflect on user feedback.** When the user corrects your approach or gives guidance on style, patterns, or organization, consider whether it's a general preference that would apply to future work. If so, suggest adding it to this file.

## Version Control

### Commits

- Use `meta(ai):` prefix for commits changing agent instruction files (`AGENTS.md`, `CLAUDE.md`, `.claude/` config, etc.).
- Use `/create-pr` to create PRs following Sentry's conventions.

### Before Creating PRs

- If the implementation is complex, ask whether to run `/code-simplifier`.
- Run `/find-bugs` to review for bugs, security, and code quality issues.

## Project Structure

- `objectstore-server/` - Web server application
- `objectstore-service/` - Core service logic and backends
- `objectstore-types/` - Shared type definitions
- `clients/rust/` - Rust client library
- `clients/python/` - Python client library
