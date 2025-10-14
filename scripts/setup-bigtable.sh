#!/bin/bash
set -euxo pipefail

docker exec objectstore-bigtable-1 bash -c "
export BIGTABLE_EMULATOR_HOST=localhost:8086
cbt -project testing -instance objectstore deletetable objectstore || true
cbt -project testing -instance objectstore createtable objectstore families=fg,fm
cbt -project testing -instance objectstore setgcpolicy objectstore fg maxage=1s
"
