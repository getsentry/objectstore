#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

BINARY=${1:-objectstore}
DEBUG_IMAGE=${DEBUG_IMAGE:-false}
case "$BINARY" in
objectstore)
    PACKAGE="objectstore-server"
    FEATURES="profiling"
    ;;
*)
    PACKAGE="$BINARY"
    FEATURES=""
    ;;
esac

BUILD_ARGS=()
if [[ "$DEBUG_IMAGE" =~ ^(true|1|yes|on)$ ]]; then
    # The hardened base image has no shell; use distroless with busybox instead.
    BUILD_ARGS+=(--build-arg BASE_IMAGE="gcr.io/distroless/cc-debian13:debug-nonroot")
fi

docker build \
    --platform linux/arm64 \
    -f Dockerfile.cross \
    -t objectstore-build \
    .

docker run --rm \
    --platform linux/arm64 \
    -e CARGO_PROFILE_RELEASE_DEBUG="${CARGO_PROFILE_RELEASE_DEBUG:-false}" \
    -v "$(pwd)":/workspace \
    -v "$HOME/.cargo/registry":/usr/local/cargo/registry \
    -v "$HOME/.cargo/git":/usr/local/cargo/git \
    objectstore-build \
    -p "$PACKAGE" --features "$FEATURES"

docker build \
    --platform linux/amd64 \
    -f Dockerfile \
    ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"} \
    --build-arg BINARY="$BINARY" \
    -t "$BINARY:latest" \
    "target/x86_64-unknown-linux-gnu/release/"
