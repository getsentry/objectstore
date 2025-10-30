#!/bin/bash
set -euo pipefail

gcloud beta emulators bigtable start --host-port=0.0.0.0:8086 &
EMULATOR_PID=$!

cbt() {
  command cbt -project testing -instance objectstore "$@"
}

until cbt ls > /dev/null 2>&1; do
  sleep 1
done

cbt deletetable objectstore || true
cbt createtable objectstore families=fg,fm
cbt setgcpolicy objectstore fg maxage=1s

wait $EMULATOR_PID
