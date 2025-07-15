# Objectstore

Objectstore is a data storage platform for blobs, files, and other unstructured
data at Sentry. It comprises a service with an RPC interface that internally
manages multiple backends, client libraries for easy integration, and a set of
utilities to manage stored data.

<img width="797" height="1075" alt="overview" src="https://github.com/user-attachments/assets/e9a4df55-591c-495f-b2a6-60d76d49958e" />

## Internals

The platform is split into the following core components:

- `api`: Contains protobuf definitions for the `client` <-> `server` protocol
  and generated code.
- `client`: The Rust client library SDK, which exposes high-performance blob
  storage access. A Python library is planned but not yet available.
- `server`: An `HTTP` server that exposes blob storage and calls functionality
  from the internal services.
- `service`: The core object storage logic.

Additionally, it contains a number of utilities:

- `stresstest`: A stresstest binary that can run various workloads against
  storage backends.

## Development

Ensure `protoc` and the latest stable Rust toolchain are installed on your
machine. Then, run the server with:

```sh
cargo run
```

To run tests:

```sh
cargo test --workspace --all-features
```

We recommend using Rust Analyzer and clippy.

## Utilities

Run the stress test binary against the running server with:

```sh
cargo run -p stresstest
```
