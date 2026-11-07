#!/bin/bash
# Borra TODOS los datos: volúmenes Docker o el directorio del cluster local.
# Ojo, esto no se puede deshacer.
cd "$(dirname "$0")/.."
if [ "$1" == "--local" ]; then rm -rf ${DFSHA_RUN:-/tmp/dfsha_run}; else docker compose down -v; fi
rm -f data-node/storage/*.blk
