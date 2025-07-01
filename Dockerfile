FROM rust:slim-bookworm AS build-chef

WORKDIR /work
RUN cargo install cargo-chef --locked

FROM build-chef AS build-planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin server

FROM build-chef AS build-server

ARG CARGO_FEATURES=""
ENV CARGO_FEATURES=${CARGO_FEATURES}

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build-planner /work/recipe.json recipe.json

RUN cargo chef cook --release --features=${CARGO_FEATURES} --recipe-path recipe.json

COPY . .

# NOTE: Simplified build that leaves debug info in the binary. As the binary grows, we will want to
# move this out and instead upload it directly to the registry and Sentry.
RUN cargo build --release --features=${CARGO_FEATURES} --bin server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates gosu \
    && rm -rf /var/lib/apt/lists/*

ENV \
    SERVER_UID=10001 \
    SERVER_GID=10001

# Create a new user and group with fixed uid/gid
RUN groupadd --system fss --gid $SERVER_GID \
    && useradd --system --gid fss --uid $SERVER_UID fss

VOLUME ["/data"]
EXPOSE 50051

COPY --from=build-server /work/target/release/server /bin
COPY docker-entrypoint.sh /docker-entrypoint.sh

ENTRYPOINT ["/bin/bash", "/docker-entrypoint.sh"]
