"""Hilo del líder: detección de DN caídos, re-replicación, reconciliación de reportes, GC y expiración."""
import logging, random, threading, time, uuid
from concurrent import futures

import data_node_pb2 as dpb
import data_node_pb2_grpc as drpc
from shared import config, constants as C
from shared.security import make_token, open_channel

log = logging.getLogger("ms.replication")


class ReplicationManager:
    def __init__(self, repo, registry):
        self.repo, self.registry = repo, registry
        self.inflight, self.led_since = {}, None
        self._stubs = {}
        self.pool = futures.ThreadPoolExecutor(4)

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()

    def _dn(self, addr):
        if addr not in self._stubs:
            self._stubs[addr] = drpc.DataNodeServiceStub(open_channel(addr))
        return self._stubs[addr]

    def _loop(self):
        # este hilo corre en los tres MS, pero solo hace trabajo el líder
        while True:
            time.sleep(C.HEARTBEAT_INTERVAL_S)
            try:
                if self.repo.is_leader():
                    # guardamos desde cuándo somos líder para no confundir nodos nuevos con caídos
                    self.led_since = self.led_since or time.time()
                    self.tick()
                else:
                    self.led_since = None
            except Exception:
                log.exception("tick")

    # Reporte completo de un DN. Sirve para corregir diferencias que los heartbeats no ven.
    def reconcile(self, addr, reported):
        have = dict(reported)
        # si el DN dice que no tiene un bloque que acá figura, se lo sacamos;
        # si tiene uno que no figura, se lo agregamos.
        for bid, m in list(self.repo.blocks.items()):
            if addr in m["replicas"] and bid not in have:
                self.repo.raft("drop_replica", block_id=bid, addr=addr)
            elif addr not in m["replicas"] and bid in have:
                self.repo.raft("add_replica", block_id=bid, addr=addr)

        pend = {b for p in list(self.repo.pending.values()) for b in p["new"]}
        for bid, age in have.items():
            # bloques viejos que ya nadie reclama: se borran del disco
            if bid not in self.repo.blocks and bid not in pend and age > C.PENDING_TTL_S:
                self.pool.submit(self._delete, addr, bid)

    def _delete(self, addr, bid):
        try:
            self._dn(addr).DeleteBlock(dpb.DeleteRequest(block_id=bid, token=make_token(bid, "x", "ms")), timeout=10)
        except Exception as e:
            # si falla, ya se reintentará en el próximo reporte o GC
            log.warning("DeleteBlock %s en %s falló: %s", bid[:8], addr, e)

    def _replicate(self, bid, src, target):
        try:
            r = self._dn(src).ReplicateBlock(dpb.ReplicateRequest(
                block_id=bid, target=target, token=make_token(bid, "x", "ms"),
                write_token=make_token(bid, "w", "ms")), timeout=70)
            if r.ok:
                self.repo.raft("add_replica", block_id=bid, addr=target)
                log.info("réplica de %s copiada %s -> %s", bid[:8], src, target)
            else:
                log.warning("re-replicación de %s falló: %s", bid[:8], r.error)
        except Exception as e:
            log.warning("re-replicación de %s falló: %s", bid[:8], e)

    def tick(self):
        now = time.time()
        dead = self.registry.dead()

        # Un líder nuevo arranca sin heartbeats. Si pasaron más de 30 s y un DN con
        # réplicas nunca latió, lo tratamos como caído para no quedarnos esperando.
        if now - self.led_since > C.DEAD_AFTER_S:
            known = self.registry.known()
            dead |= {r for m in self.repo.blocks.values() for r in m["replicas"] if r not in known}

        alive, todo = self.registry.alive(), []
        for bid, m in list(self.repo.blocks.items()):
            for r in [x for x in m["replicas"] if x in dead]:
                self.repo.raft("drop_replica", block_id=bid, addr=r)
            live = [r for r in m["replicas"] if r in alive]
            if live and len(m["replicas"]) < C.REPLICATION_FACTOR:
                todo.append((len(live), bid, live, m))

        # Los que están con una sola copia van primero. Limitamos a 50 por vuelta
        # para no llenar el pool de tareas si se cae medio cluster.
        for _, bid, live, m in sorted(todo, key=lambda t: t[:2])[:50]:
            if now - self.inflight.get(bid, 0) < 60:
                continue
            cands = [a for a in alive if a not in m["replicas"]]
            if cands:
                self.inflight[bid] = now
                # elegimos el candidato con más espacio libre
                self.pool.submit(self._replicate, bid, random.choice(live), max(cands, key=lambda a: alive[a]["free"]))

        # Borrado físico recién después del periodo de gracia
        for t in list(self.repo.tombs):
            if now - t["ts"] > config.DELETE_GRACE_S:
                for bid in t["blocks"]:
                    for r in self.repo.blocks.get(bid, {}).get("replicas", []):
                        self.pool.submit(self._delete, r, bid)
                self.repo.raft("gc", tid=t["id"])

        # Escrituras que quedaron abiertas y ya nadie va a cerrar
        for sid, p in list(self.repo.pending.items()):
            if now - p["ts"] > C.PENDING_TTL_S:
                self.repo.raft("expire", sid=sid, tid=str(uuid.uuid4()), now=now)
