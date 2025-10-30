#!/bin/bash
set -euxo pipefail

env

exit 1

#ROOT_DIR="$(realpath $(dirname "$0")/..)"
#
#OLD_VERSION="${1}"
#NEW_VERSION="${2}"
#
#echo "Current version: $OLD_VERSION"
#echo "Bumping to version: $NEW_VERSION"
#
## =================== Rust ===================
#
#cd $ROOT_DIR
#cd objectstore-client
#
#perl -pi -e "s/^version = \".*?\"/version = \"$NEW_VERSION\"/" Cargo.toml
#cargo metadata --format-version 1 >/dev/null # update `Cargo.lock`
#
## ==================== PY ====================
#
#cd $ROOT_DIR
#cd python-objectstore-client
#
#uv version "$NEW_VERSION"
