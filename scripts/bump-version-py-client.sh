#!/bin/bash
set -euxo pipefail

ROOT_DIR="$(realpath $(dirname "$0")/..)"

OLD_VERSION="${1}"
NEW_VERSION="${2}"

echo "Current version: $OLD_VERSION"
echo "Bumping to version: $NEW_VERSION"

cd $ROOT_DIR
cd clients/python

uv version "$NEW_VERSION"
