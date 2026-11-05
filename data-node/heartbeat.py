"""Heartbeats cada 3 s y reporte completo de bloques (al arrancar y cada hora) hacia el líder de MS."""
import logging, time

import metadata_pb2 as mpb
import metadata_pb2_grpc as mrpc
from shared import config, constants as C
from shared.exceptions import DFSError
from shared.utils import LeaderRouter

log = logging.getLogger("dn.heartbeat")


class HeartbeatClient:
    def __init__(self, addr, storage, ms_seeds):
        self.addr, self.storage = addr, storage
        # El router se acuerda del líder para no preguntar a las tres semillas cada vez.
        self.router = LeaderRouter(ms_seeds)

    def _call(self, method, req):
        # rounds=2: si el líder está cambiando, no queremos bloquear el hilo mucho tiempo
        return self.router.call(mrpc.MetadataServiceStub, method, req, timeout=10, rounds=2)

    def send_block_report(self):
        # El reporte completo es más caro que un heartbeat, por eso va cada hora.
        req = mpb.BlockReportRequest(addr=self.addr)
        req.blocks.extend(mpb.BlockInfo(block_id=b, age_s=a) for b, a in self.storage.list_blocks())
        return self._call("BlockReport", req).status.code == "OK"

    def run(self):
        last_report = 0.0
        while True:
            try:
                self._call("Heartbeat", mpb.HeartbeatRequest(addr=self.addr, free_bytes=self.storage.free_bytes()))
                # Al arrancar last_report vale 0, así que el primer reporte sale enseguida.
                if time.time() - last_report > config.BLOCK_REPORT_INTERVAL_S and self.send_block_report():
                    last_report = time.time()
            except DFSError:
                # Si no hay líder, seguimos intentando. No queremos que el hilo muera.
                log.warning("sin líder de metadatos, reintentando")
            time.sleep(C.HEARTBEAT_INTERVAL_S)
