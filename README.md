# Foundational Storage

This repository contains code related to the foundational storage team / initiative.

It currently contains the following pieces:
- `api`: Contains protobuf / `gRPC` definitions for the `client`<->`server` protocol and generated code.
- `client`: The client library SDK, which exposes high-performance blob storage access.
- `server`: A `gRPC` / `HTTP` server that exposes blob storage towards the `client` library,
  as well as external clients.
- `service`: The core blob storage primitives.
- `stresstest`: A stresstest binary that can run various workloads against a storage backend.

## Development

Ensure `protoc` and the latest stable Rust toolchain are installed on your machine. Then, run the server with:

```sh
cargo run
```

Run the stress test binary against the running server with:
```sh
cargo run -p stresstest
```

To run tests:

```sh
cargo test --workspace --all-features
```

We recommend using Rust Analyzer and clippy.
