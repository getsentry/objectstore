#!/bin/bash
set -euxo pipefail

CONTAINER_NAME=${1:-objectstore-bigtable-1}

docker exec "$CONTAINER_NAME" bash -c "
export BIGTABLE_EMULATOR_HOST=localhost:8086
cbt -project testing -instance objectstore deletetable objectstore || true
cbt -project testing -instance objectstore createtable objectstore families=fg,fm
cbt -project testing -instance objectstore setgcpolicy objectstore fg maxage=1s
"
