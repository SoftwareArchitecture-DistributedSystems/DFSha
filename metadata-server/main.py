#!/usr/bin/env python3
"""Servidor de metadatos (MS) con estado replicado por Raft."""
import argparse, logging
from concurrent import futures

import grpc

import auth_pb2_grpc as arpc
import metadata_pb2_grpc as mrpc
from block_manager import BlockManager, DataNodeRegistry
from file_manager import FileManager
from metadata_service import AuthService, MetadataService
from namespace_manager import NamespaceManager
from replication_manager import ReplicationManager
from shared import constants as C
from shared.utils import setup_logging, start_grpc_server
from storage.metadata_repository import MetadataRepository
from user_manager import UserManager


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self", dest="me", required=True, help="host:puerto_raft de este MS")
    ap.add_argument("--peers", required=True, help="host:puerto_raft de los otros MS, separados por coma")
    ap.add_argument("--data", default="metadata-server/storage/data")
    a = ap.parse_args()
    setup_logging()

    # El repositorio es el objeto replicado por Raft. Los managers son solo la lógica
    # que arma las operaciones; el estado vive en el repo.
    repo = MetadataRepository(a.me, a.peers.split(","), a.data)
    registry = DataNodeRegistry()
    users, ns, files = UserManager(repo), NamespaceManager(repo), FileManager(repo)
    blocks, repl = BlockManager(repo, files, registry), ReplicationManager(repo, registry)

    server = grpc.server(futures.ThreadPoolExecutor(32))
    arpc.add_AuthServiceServicer_to_server(AuthService(repo, users), server)
    mrpc.add_MetadataServiceServicer_to_server(
        MetadataService(repo, users, ns, files, blocks, registry, repl), server)

    # El puerto de Raft es el que se pasa en --self; gRPC escucha 100 puertos más arriba.
    port = int(a.me.rsplit(":", 1)[1]) + C.MS_GRPC_OFFSET
    start_grpc_server(server, port)
    repl.start()
    logging.getLogger("ms").info("MS %s listo (gRPC %d)", a.me, port)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
