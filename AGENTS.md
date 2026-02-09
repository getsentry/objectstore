# Claude Code Instructions for Objectstore

This file contains project-specific guidance for Claude Code when working on the Objectstore project.

## Running Tests

### Standard Test Command

Always use the full workspace test command to ensure all packages and features are tested:

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

## Linting

Always run linting checks before committing code. Use the same commands as CI:

### Rust Linting

Check for compilation errors and clippy lints:

```bash
cargo clippy --workspace --all-targets --all-features --no-deps
```

### Documentation Validation

When adding docs or moving types, verify documentation references:

```bash
cargo doc --workspace --all-features --no-deps --document-private-items
```

### Python Linting

For Python code, run both linting and type checking:

```bash
uv run ruff check
uv run mypy .
```

## Creating Pull Requests

Before creating a PR, follow this workflow to ensure code quality:

### 1. Review for Issues

Always use the `find-bugs` skill from `sentry-skills` to review changes and address potential issues:

```
/find-bugs
```

This performs a comprehensive review of your changes for bugs, security vulnerabilities, and code quality issues.

### 2. Check Code Complexity

Review the code you added or modified. If the implementation is complex:
- Suggest the user run `/code-simplifier` to improve clarity and maintainability
- The `code-simplifier` skill (from `sentry-skills`) refines code while preserving functionality

### 3. Create the PR

Use the `create-pr` skill from `sentry-skills` to create PRs following Sentry's conventions:

```
/create-pr
```

This ensures the PR follows Sentry's standards for title, description, and formatting.

## Project Structure

- `objectstore-server/` - Web server application
- `objectstore-service/` - Core service logic and backends
- `objectstore-types/` - Shared type definitions
- `clients/rust/` - Rust client library
- `clients/python/` - Python client library
