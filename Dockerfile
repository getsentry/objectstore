## BUILD

FROM rust:slim-bookworm AS build-chef

WORKDIR /work
RUN cargo install cargo-chef --locked

FROM build-chef AS build-base

ARG CARGO_FEATURES=""
ENV CARGO_FEATURES=${CARGO_FEATURES}

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends protobuf-compiler libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

## RUNTIME

FROM debian:bookworm-slim AS runtime-base

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

## server

FROM build-chef AS server-planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin server

FROM build-base AS build-server
COPY --from=server-planner /work/recipe.json recipe.json
RUN cargo chef cook --release --features=${CARGO_FEATURES} --recipe-path recipe.json

COPY . .

RUN cargo build --release --features=${CARGO_FEATURES} --bin server

FROM runtime-base AS server

ENV FSS_PATH="/data"
VOLUME ["/data"]

COPY --from=build-server /work/target/release/server /bin

ENTRYPOINT ["/bin/server"]
EXPOSE 50051
EXPOSE 8888

## stresstest

FROM build-chef AS stresstest-planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin stresstest

FROM build-base AS build-stresstest
COPY --from=stresstest-planner /work/recipe.json recipe.json
RUN cargo chef cook --release --features=${CARGO_FEATURES} --recipe-path recipe.json

COPY . .

RUN cargo build --release --features=${CARGO_FEATURES} --package stresstest --bin stresstest

FROM runtime-base AS stresstest
COPY --from=build-stresstest /work/target/release/stresstest /bin
ENTRYPOINT ["/bin/stresstest"]
