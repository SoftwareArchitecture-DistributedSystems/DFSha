"""Cliente gRPC del plano de datos. Escribe en cadena y verifica el SHA-256 al leer."""
import sys, time

import grpc

import data_node_pb2 as dpb
import data_node_pb2_grpc as drpc
from shared import constants as C
from shared.exceptions import DATA_LOSS, DFSError, INSUFFICIENT_REPLICAS
from shared.security import open_channel
from shared.utils import sha256_hex, split_chunks


class DataNodeClient:
    def __init__(self):
        self._stubs = {}

    def _stub(self, addr):
        # un stub por DN; los canales de gRPC son caros de crear
        if addr not in self._stubs:
            self._stubs[addr] = drpc.DataNodeServiceStub(open_channel(addr))
        return self._stubs[addr]

    @staticmethod
    def _chunks(data, alloc, sha, pipeline):
        for i, part in enumerate(split_chunks(data)):
            c = dpb.BlockChunk(data=part)
            if i == 0:
                # la cabecera viaja solo en el primer chunk; los demás son datos puros
                c.block_id, c.token, c.sha256, c.version = alloc.block_id, alloc.write_token, sha, alloc.version
                c.next.extend(pipeline[1:])
            yield c

    def write_block(self, alloc, data, sha):
        """Manda el bloque al primer DN de la cadena. Si un eslabón falla, lo saca y reintenta.
        Devuelve los DN que confirmaron; el llamador después exige W = 2."""
        pl = list(alloc.pipeline)
        while len(pl) >= C.WRITE_QUORUM:
            try:
                ack = self._stub(pl[0]).WriteBlock(self._chunks(data, alloc, sha, pl), timeout=60)
            except grpc.RpcError:
                # el primer DN no responde: lo descartamos y probamos con el siguiente
                pl = pl[1:]
                continue
            if len(ack.acked) >= C.WRITE_QUORUM and not ack.error:
                return list(ack.acked)
            # nos fijamos cuál fue el primer nodo que no confirmó y lo sacamos de la cadena
            missing = next((n for n in pl if n not in ack.acked), None)
            pl = [n for n in pl if n != missing] if missing else pl[:1]
        raise DFSError(INSUFFICIENT_REPLICAS, "no se alcanzó el quorum de escritura")

    def read_block(self, loc):
        """Prueba las réplicas en orden. Si una falla o devuelve otro hash, pasa a la siguiente."""
        for rep in loc.replicas:
            try:
                parts = self._stub(rep).ReadBlock(dpb.ReadRequest(block_id=loc.block_id, token=loc.read_token), timeout=60)
                data = b"".join(c.data for c in parts)
                if sha256_hex(data) == loc.sha256:
                    return data
                # el dato llegó, pero no es el que esperábamos: puede ser una réplica corrupta
                print(f"[aviso] hash inválido en {rep}, probando otra réplica", file=sys.stderr)
            except grpc.RpcError:
                print(f"[aviso] {rep} no responde, probando otra réplica", file=sys.stderr)
        return None
