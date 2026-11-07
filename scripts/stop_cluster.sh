#!/bin/bash
cd "$(dirname "$0")/.."
if [ "$1" == "--local" ]; then
  # Matamos los procesos que arrancó start_cluster.sh y borramos los pid files.
  RUN=${DFSHA_RUN:-/tmp/dfsha_run}
  for f in $RUN/*.pid; do [ -f "$f" ] && kill $(cat $f) 2>/dev/null; done; rm -f $RUN/*.pid
else
  # Los volúmenes quedan; para borrarlos está clean_storage.sh.
  docker compose down
fi
