FROM rust:slim-bookworm AS build-chef

WORKDIR /work
RUN cargo install cargo-chef --locked

FROM build-chef AS build-planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin objectstore

FROM build-chef AS build-server

ARG CARGO_FEATURES=""
ENV CARGO_FEATURES=${CARGO_FEATURES}

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build-planner /work/recipe.json recipe.json

RUN cargo chef cook --release --features=${CARGO_FEATURES} --recipe-path recipe.json

COPY . .

# NOTE: Simplified build that leaves debug info in the binary. As the binary grows, we will want to
# move this out and instead upload it directly to the registry and Sentry.
RUN cargo build --release --features=${CARGO_FEATURES} --bin objectstore

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV FSS_PATH="/data"
VOLUME ["/data"]

COPY --from=build-server /work/target/release/objectstore /bin

ENTRYPOINT ["/bin/objectstore"]
EXPOSE 8888
