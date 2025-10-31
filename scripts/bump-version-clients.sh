#!/bin/bash
set -euxo pipefail

ROOT_DIR="$(realpath $(dirname "$0")/..)"

OLD_VERSION="${1}"
NEW_VERSION="${2}"

echo "Current version: $OLD_VERSION"
echo "Bumping to version: $NEW_VERSION"

# =================== Rust ===================

cd $ROOT_DIR
cd clients/rust

perl -pi -e "s/^version = \".*?\"/version = \"$NEW_VERSION\"/" Cargo.toml
cargo metadata --format-version 1 >/dev/null # update `Cargo.lock`

# ==================== PY ====================

cd $ROOT_DIR
cd clients/python

uv version "$NEW_VERSION"
