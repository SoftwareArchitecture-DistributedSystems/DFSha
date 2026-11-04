"""Replicación en cadena (pipeline) y copia de réplicas ordenada por MS."""
import logging

import grpc

import data_node_pb2 as dpb
import data_node_pb2_grpc as drpc
from shared.security import open_channel
from shared.utils import sha256_hex, split_chunks

log = logging.getLogger("dn.replication")


def block_chunks(data, block_id, token, next_addrs, sha, version):
    for i, part in enumerate(split_chunks(data)):
        c = dpb.BlockChunk(data=part)
        if i == 0:
            # la cabecera va solo en el primer chunk, igual que en el cliente
            c.block_id, c.token, c.sha256, c.version = block_id, token, sha, version
            c.next.extend(next_addrs)
        yield c


def forward(next_addrs, data, block_id, token, sha, version):
    """Reenvía el bloque al siguiente eslabón. Devuelve los DN que confirmaron aguas abajo."""
    if not next_addrs:
        # somos el último de la cadena
        return []
    try:
        stub = drpc.DataNodeServiceStub(open_channel(next_addrs[0]))
        ack = stub.WriteBlock(block_chunks(data, block_id, token, list(next_addrs[1:]), sha, version), timeout=60)
        return list(ack.acked)
    except grpc.RpcError as e:
        # No cortamos la escritura acá: el cliente se encarga de rearmar la cadena
        # con los nodos que sí respondieron.
        log.warning("cadena rota hacia %s: %s", next_addrs[0], e.code())
        return []


def push_block(storage, block_id, target, write_token) -> bool:
    """ReplicateBlock: lee la réplica local y se la manda a target."""
    version, data = storage.load(block_id)
    stub = drpc.DataNodeServiceStub(open_channel(target))
    ack = stub.WriteBlock(block_chunks(data, block_id, write_token, [], sha256_hex(data), version), timeout=60)
    return bool(ack.acked) and not ack.error
