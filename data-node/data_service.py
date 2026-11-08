"""Servicio gRPC del nodo de datos."""
import logging

import grpc

import data_node_pb2 as dpb
import data_node_pb2_grpc as drpc
import integrity, replication
from shared import constants as C
from shared.security import check_token
from shared.utils import split_chunks

log = logging.getLogger("dn.service")


class DataNodeService(drpc.DataNodeServiceServicer):
    def __init__(self, addr, storage):
        self.addr, self.storage = addr, storage

    def WriteBlock(self, request_iterator, ctx):
        # El primer chunk trae la cabecera (id, token, hash, versión y próximos nodos).
        # Los siguientes son solo datos. Ojo: juntamos todo en memoria; para bloques
        # de 8 MiB nos alcanza, pero no es lo ideal para bloques gigantes.
        try:
            first = next(request_iterator)
        except StopIteration:
            # Un cliente que cierra el stream antes de la cabecera no debe tumbar el DN.
            return dpb.WriteAck(error="INVALID")
        if not check_token(first.token, first.block_id, "w"):
            return dpb.WriteAck(error="PERMISSION_DENIED")
        data = first.data + b"".join(c.data for c in request_iterator)
        if not integrity.verify_hash(data, first.sha256):
            return dpb.WriteAck(error="HASH_MISMATCH")

        self.storage.store(first.block_id, first.version, data)
        # El ack propio va primero; después se agregan los de los nodos de aguas abajo.
        acked = [self.addr]
        acked += replication.forward(list(first.next), data, first.block_id, first.token, first.sha256, first.version)
        return dpb.WriteAck(acked=acked)

    def ReadBlock(self, req, ctx):
        if not check_token(req.token, req.block_id, "r"):
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, "token inválido")
        try:
            _, data = self.storage.load(req.block_id)
        except Exception:
            # el bloque puede no existir o estar corrupto; para el cliente es lo mismo
            ctx.abort(grpc.StatusCode.NOT_FOUND, "bloque ausente o corrupto")
        for part in split_chunks(data, C.CHUNK_SIZE):
            yield dpb.BlockChunk(data=part)

    def ReplicateBlock(self, req, ctx):
        # Esta operación la ordena el MS, no un cliente. Por eso el token es "x".
        if not check_token(req.token, req.block_id, "x"):
            return dpb.ReplicateReply(ok=False, error="PERMISSION_DENIED")
        try:
            ok = replication.push_block(self.storage, req.block_id, req.target, req.write_token)
            return dpb.ReplicateReply(ok=ok, error="" if ok else "TARGET_REJECTED")
        except Exception as e:
            return dpb.ReplicateReply(ok=False, error=str(e)[:200])

    def DeleteBlock(self, req, ctx):
        if not check_token(req.token, req.block_id, "x"):
            return dpb.DeleteReply(deleted=False)
        return dpb.DeleteReply(deleted=self.storage.delete(req.block_id))

    def GetBlockStatus(self, req, ctx):
        if not check_token(req.token, req.block_id, "x"):
            return dpb.StatusReply()
        try:
            version, data = self.storage.load(req.block_id)
            return dpb.StatusReply(exists=True, size=len(data), version=version, healthy=True)
        except FileNotFoundError:
            return dpb.StatusReply(exists=False)
        except Exception:
            # existe pero no se pudo descifrar: probablemente corrupto
            return dpb.StatusReply(exists=True, healthy=False)
