#!/bin/bash
set -euo pipefail

docker-entrypoint.sh cassandra -f &
CASSANDRA_PID=$!
trap 'kill "$CASSANDRA_PID" 2>/dev/null || true' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

ready=false
STARTUP_DEADLINE=$((SECONDS + 180))
while (( SECONDS < STARTUP_DEADLINE )); do
  kill -0 "$CASSANDRA_PID" 2>/dev/null || exit 1
  if cqlsh --connect-timeout=2 --request-timeout=5 localhost -e 'SELECT release_version FROM system.local' >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done
if [ "$ready" != true ]; then
  echo 'Cassandra did not become ready within the startup deadline' >&2
  exit 1
fi

cqlsh localhost -e "CREATE KEYSPACE IF NOT EXISTS objectstore WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}"
cqlsh localhost -k objectstore -f /schema.cql
wait "$CASSANDRA_PID"
