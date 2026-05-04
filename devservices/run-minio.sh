#!/bin/bash
set -euo pipefail

mkdir -p /data/test-bucket

minio server /data --address :9000 &
MINIO_PID=$!

for _ in {1..4}; do
    mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && break
    sleep 1
done

mc anonymous set public local/test-bucket

wait $MINIO_PID
