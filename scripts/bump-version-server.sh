#!/bin/bash
set -euxo pipefail

ROOT_DIR="$(realpath $(dirname "$0")/..)"

OLD_VERSION="${1}"
NEW_VERSION="${2}"

echo "Current version: $OLD_VERSION"
echo "Bumping to version: $NEW_VERSION"

cd $ROOT_DIR
cd objectstore-server
perl -pi -e "s/^version = \".*?\"/version = \"$NEW_VERSION\"/" Cargo.toml

cd $ROOT_DIR
cargo metadata --format-version 1 >/dev/null # update `Cargo.lock`
