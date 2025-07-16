# Objectstore

Objectstore is a data storage platform for blobs, files, and other unstructured
data at Sentry. It comprises a service with an RPC interface that internally
manages multiple backends, client libraries for easy integration, and a set of
utilities to manage stored data.

<img width="531" height="716" alt="overview" src="https://github.com/user-attachments/assets/e9a4df55-591c-495f-b2a6-60d76d49958e" />

## Internals

The platform is split into the following core components:

- `objectstore-api`: Contains protobuf definitions for the `client` <-> `server` protocol
  and generated code.
- `objectstore-client`: The Rust client library SDK, which exposes high-performance blob
  storage access. A Python library is planned but not yet available.
- `objectstore-server`: An `HTTP` server that exposes blob storage and calls
  functionality from the internal services. This crate creates the `objectstore`
  binary.
- `objectstore-service`: The core object storage logic.

Additionally, it contains a number of utilities:

- `stresstest`: A stresstest binary that can run various workloads against
  storage backends.

## Building

Ensure `protoc` and the latest stable Rust toolchain are installed on your
machine. A release build can be created with:

```sh
cargo build --release
```

The server binary will be located at `target/release/objectstore`.

## Development

You can run a development build of the server with this command:

```sh
cargo run
```

To run tests:

```sh
cargo test --workspace --all-features
```

We recommend using Rust Analyzer and clippy, which will apply formatting and
warn about lints during development. To do this manually, run:

```sh
# Check and fix formatting
cargo fmt

# Lint all features
cargo clippy --workspace --all-features
```

## Utilities

Run the stress test binary against the running server with:

```sh
cargo run --release -p stresstest
```
