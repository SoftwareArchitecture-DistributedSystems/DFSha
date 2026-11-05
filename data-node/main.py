#!/usr/bin/env python3
"""Nodo de datos (DN)."""
import argparse, logging, threading
from concurrent import futures

import grpc

import data_node_pb2_grpc as drpc
import integrity
from block_storage import BlockStorage
from data_service import DataNodeService
from heartbeat import HeartbeatClient
from shared import config
from shared.security import GRPC_OPTS
from shared.utils import setup_logging, start_grpc_server


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True, help="host:puerto anunciado a clientes y otros DN")
    ap.add_argument("--ms", default=config.DEFAULT_MS, help="host:puerto_grpc de los MS, separados por coma")
    ap.add_argument("--data", default="data-node/storage")
    a = ap.parse_args()
    setup_logging()

    # La clave maestra llega en hex porque es más fácil pasarla por variable de entorno.
    storage = BlockStorage(a.data, bytes.fromhex(config.MASTER_KEY_HEX))
    server = grpc.server(futures.ThreadPoolExecutor(32), options=GRPC_OPTS)
    drpc.add_DataNodeServiceServicer_to_server(DataNodeService(a.addr, storage), server)
    start_grpc_server(server, int(a.addr.rsplit(":", 1)[1]))

    # Dos hilos de fondo: uno manda heartbeats y el otro revisa los bloques cada tanto.
    threading.Thread(target=HeartbeatClient(a.addr, storage, a.ms.split(",")).run, daemon=True).start()
    threading.Thread(target=integrity.scrub_loop, args=(storage,), daemon=True).start()

    logging.getLogger("dn").info("DN %s listo", a.addr)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
