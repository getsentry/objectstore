# Foundational Storage

This repository contains code related to the foundational storage team / initiative.

It currently contains the following pieces:
- `client`: The client library SDK, which exposes high-performance blob storage access.
- `proto`: Protobuf / `gRPC` definitions for the `client`<->`server` protocol.
- `proto-codegen`: Just a crate containing the code generated from the `proto` definitions.
- `server`: A `gRPC` / `HTTP` server that exposes blob storage towards the `client` library,
  as well as external clients.
- `service`: The core blob storage primitives.
- `stresstest`: A stresstest binary that can run various workloads against a storage backend.
