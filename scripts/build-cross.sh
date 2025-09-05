#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

BINARY=${1:-objectstore}
case "$BINARY" in
    objectstore) PACKAGE="objectstore-server" ;;
    *) PACKAGE="$BINARY" ;;
esac

docker build \
    --platform linux/arm64 \
    -f Dockerfile.cross \
    -t objectstore-build \
    .

docker run --rm \
    --platform linux/arm64 \
    -v "$(pwd)":/workspace \
    -v "$HOME/.cargo/registry":/usr/local/cargo/registry \
    -v "$HOME/.cargo/git":/usr/local/cargo/git \
    objectstore-build \
    -p "$PACKAGE"

docker build \
    --platform linux/amd64 \
    -f Dockerfile \
    --build-arg BINARY="$BINARY" \
    -t "$BINARY:latest" \
    "target/x86_64-unknown-linux-gnu/release/"
