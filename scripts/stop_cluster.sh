#!/bin/bash
cd "$(dirname "$0")/.."
if [ "$1" == "--local" ]; then
  # Matamos los procesos que arrancó start_cluster.sh y borramos los pid files.
  RUN=${DFSHA_RUN:-/tmp/dfsha_run}
  PIDS=""
  for f in $RUN/*.pid; do [ -f "$f" ] && PIDS="$PIDS $(cat $f)"; done
  [ -n "$PIDS" ] && kill $PIDS 2>/dev/null

  # Si un pid file quedó viejo (por ejemplo, tras un arranque duplicado), matamos
  # igual los procesos que usan este mismo directorio de datos.
  pkill -f "metadata-server/main.py .*--data $RUN/ms" 2>/dev/null
  pkill -f "data-node/main.py .*--data $RUN/dn" 2>/dev/null

  # Esperamos a que liberen los puertos antes de dar el clúster por detenido.
  for _ in $(seq 1 40); do
    pgrep -f "metadata-server/main.py .*--data $RUN/ms" >/dev/null 2>&1 && { sleep 0.25; continue; }
    pgrep -f "data-node/main.py .*--data $RUN/dn" >/dev/null 2>&1 && { sleep 0.25; continue; }
    break
  done
  pkill -9 -f "metadata-server/main.py .*--data $RUN/ms" 2>/dev/null
  pkill -9 -f "data-node/main.py .*--data $RUN/dn" 2>/dev/null
  rm -f $RUN/*.pid
else
  # Los volúmenes quedan; para borrarlos está clean_storage.sh.
  docker compose down
fi
