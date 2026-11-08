#!/bin/bash
# Uso: scripts/start_cluster.sh            -> Docker Compose (3 MS + 5 DN + cliente), con mTLS
#      scripts/start_cluster.sh --local    -> procesos locales sin Docker (127.0.0.1, sin TLS)
cd "$(dirname "$0")/.."
ROOT=$(pwd)
if [ "$1" == "--local" ]; then
  # Modo local: todo corre en la misma máquina, sin TLS. Sirve para probar rápido.
  RUN=${DFSHA_RUN:-/tmp/dfsha_run}; mkdir -p $RUN
  # Arranque idempotente: si quedó un clúster de una corrida anterior (o una prueba
  # de fallos dejó nodos detenidos), lo bajamos para no chocar puertos ni arrastrar
  # un clúster incompleto.
  export DFSHA_RUN=$RUN
  scripts/stop_cluster.sh --local
  export PYTHONPATH=$ROOT:$ROOT/proto/generated
  scripts/generate_proto.sh >/dev/null

  # Tres servidores de metadatos. Los puertos Raft son 7000, 7001 y 7002;
  # el gRPC de cada uno queda en 7100, 7101 y 7102.
  for i in 0 1 2; do
    PEERS=$(for j in 0 1 2; do [ $j != $i ] && echo -n "127.0.0.1:$((7000+j)),"; done | sed 's/,$//')
    setup="setsid nohup python3 metadata-server/main.py --self 127.0.0.1:$((7000+i)) --peers $PEERS --data $RUN/ms$i"
    $setup > $RUN/ms$i.log 2>&1 & echo $! > $RUN/ms$i.pid
  done

  MS=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
  # Cinco nodos de datos en 7201..7205
  for i in 1 2 3 4 5; do
    setsid nohup python3 data-node/main.py --addr 127.0.0.1:720$i --ms $MS --data $RUN/dn$i > $RUN/dn$i.log 2>&1 &
    echo $! > $RUN/dn$i.pid
  done
  # Esperamos a que los 8 servicios acepten conexiones antes de devolver el control.
  OK=1
  for _ in $(seq 1 60); do
    OK=1
    for p in 7100 7101 7102 7201 7202 7203 7204 7205; do
      (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null || { OK=0; break; }
    done
    [ $OK = 1 ] && break
    sleep 0.5
  done
  if [ $OK = 1 ]; then
    echo "clúster local en marcha (logs en $RUN). export DFSHA_MS=$MS"
  else
    echo "ADVERTENCIA: no todos los servicios aceptaron conexiones; revisa $RUN/*.log" >&2
  fi
else
  # Modo Docker: generamos certificados y secretos solo la primera vez.
  [ -d certs ] || scripts/generate_certs.sh certs ms1 ms2 ms3 dn1 dn2 dn3 dn4 dn5 client
  if [ ! -f .env ]; then
    { echo "TOKEN_SECRET=$(openssl rand -hex 32)"; echo "JWT_SECRET=$(openssl rand -hex 32)"
      echo "RAFT_PASSWORD=$(openssl rand -hex 16)"
      for i in 1 2 3 4 5; do echo "DN${i}_KEY=$(openssl rand -hex 32)"; done; } > .env
  fi
  docker compose up -d --build
  echo "clúster en marcha. Cliente: docker compose exec client python client/main.py login <usuario>"
fi
