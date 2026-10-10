#!/bin/bash
set -euo pipefail

docker-entrypoint.sh cassandra -f &
CASSANDRA_PID=$!

until cqlsh -e "describe keyspaces" >/dev/null 2>&1; do
  sleep 2
done

cqlsh -f /cql-schema.cql

wait $CASSANDRA_PID
