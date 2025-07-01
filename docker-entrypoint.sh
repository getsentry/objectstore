#!/usr/bin/env bash
set -eu

if [ "$(id -u)" == "0" ]; then
  exec gosu fss /bin/server "$@"
else
  exec /bin/server "$@"
fi
