# Agent Instructions for Objectstore

This file contains project-specific guidance for agents when working on the Objectstore project.

## Running Tests

### Standard Test Command

When running tests, use the full workspace test command to ensure all packages and features are tested:

```bash
cargo test --workspace --all-features
```

### Testing Individual Packages

To test only one package (e.g., when backend services aren't needed):

```bash
cargo test -p objectstore-server --all-features
```

## Adding Dependencies

Rust dependencies are declared once in the root `Cargo.toml` under
`[workspace.dependencies]` with a full version, and referenced from crates as
`{ workspace = true }`. Only pin a version in a crate's own `Cargo.toml` when
that crate needs to diverge, and say why in a comment.

## Documentation Conventions

**Keep docs at the right level**:
- Crate-level docs (`docs/architecture.md` where present, otherwise the crate's top-level doc) are the highest-level design and behavior reference: what a newcomer reads first to see if/how the crate fits their need, plus pointers into lower-level docs. Update them when something changes at that level, or when a new pointer to lower-level docs is needed. Keep them scoped to role and key concepts.
- A concept's or module's behavior belongs on that module's doc comment where a module exists, or otherwise on the root type that represents it — not folded into the crate-level doc.
- Code-level detail (a specific type's or method's exact behavior) belongs on that type's or method's own doc comment: a short one-line summary first, then detail, then a code example and any edge cases (panics, errors).

## Before Responding to the User

Do these checks after completing a batch of edits, before handing control back to the user. Do not defer them to commit time.

### Lint Rust

After editing Rust files, run formatting and clippy:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features --no-deps
```

Fix any issues before responding.

### Lint Python

After editing Python files, run formatting, linting, and type checking:

```bash
uv run ruff format
uv run ruff check
uv run mypy .
```

Fix any issues before responding.

### Documentation

If your changes affect documented behavior, search `docs/` and doc comments for terms related to your change across every crate you touched. Update them according to the project conventions.

When adding docs or moving types, verify documentation references:

```bash
cargo doc --workspace --all-features --no-deps --document-private-items
```
