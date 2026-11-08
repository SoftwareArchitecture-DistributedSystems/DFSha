"""Registro de nodos de datos, asignación de bloques (placement) y localización de réplicas."""
import random, threading, time, uuid

from shared import constants as C
from shared.exceptions import DFSError, INSUFFICIENT_REPLICAS, NOT_FOUND, BLOCK_LOCKED
from shared.security import make_token


class DataNodeRegistry:
    """Estado de los DN según los heartbeats. Vive en memoria del líder y se pierde en una elección."""

    def __init__(self):
        self._d, self._lk = {}, threading.Lock()

    def heartbeat(self, addr, free):
        with self._lk:
            self._d[addr] = {"seen": time.time(), "free": free}

    def alive(self):
        # para asignar bloques solo contamos los que respondieron hace poco
        now = time.time()
        with self._lk:
            return {a: dict(d) for a, d in self._d.items() if now - d["seen"] < C.SUSPECT_AFTER_S}

    def dead(self):
        # un DN se considera caído recién a los 30 s; antes puede ser solo un hipo de red
        now = time.time()
        with self._lk:
            return {a for a, d in self._d.items() if now - d["seen"] > C.DEAD_AFTER_S}

    def known(self):
        with self._lk:
            return set(self._d)

    def states(self):
        now = time.time()
        with self._lk:
            return {a: ("ALIVE" if now - d["seen"] < C.SUSPECT_AFTER_S else
                        "SUSPECT" if now - d["seen"] <= C.DEAD_AFTER_S else "DEAD") for a, d in self._d.items()}


class BlockManager:
    def __init__(self, repo, files, registry):
        self.repo, self.files, self.registry = repo, files, registry

    def allocate(self, sid, idx, uid):
        p = self.files.session(sid, uid)
        if self.files.locked_by_other(p["path"], idx, p["holder"]):
            raise DFSError(BLOCK_LOCKED, f"bloque {idx} bloqueado por otro cliente")

        live = self.registry.alive()
        if len(live) < C.WRITE_QUORUM:
            raise DFSError(INSUFFICIENT_REPLICAS, f"solo {len(live)} DN vivos")

        # Round-robin estable sobre los DN vivos: los bloques consecutivos de un archivo
        # recorren todos los nodos. Al depender del índice de bloque y no de estado en
        # memoria, la distribución se mantiene aunque cambie el líder de Raft.
        order = sorted(live)
        start = idx % len(order)
        pipe = [order[(start + k) % len(order)] for k in range(min(C.REPLICATION_FACTOR, len(order)))]

        bid = str(uuid.uuid4())
        return bid, p["base"] + 1, pipe, make_token(bid, "w", uid, C.WRITE_TOKEN_TTL_S)

    def locations(self, path, uid, offset, length):
        f = self.repo.files.get(path)
        if not f or f["type"] != "file":
            raise DFSError(NOT_FOUND, path)

        alive, out, pos = self.registry.alive(), [], 0
        end = offset + length if length > 0 else f["size"]
        for i, bid in enumerate(f["blocks"]):
            m = self.repo.blocks[bid]
            # solo devolvemos los bloques que se solapan con el rango pedido
            if pos + m["size"] > offset and pos < end:
                live = [r for r in m["replicas"] if r in alive]
                random.shuffle(live)   # las lecturas se reparten entre las réplicas vivas
                # las caídas van al final por si el cliente quiere intentar igual
                out.append((i, bid, m, pos, live + [r for r in m["replicas"] if r not in alive],
                            make_token(bid, "r", uid, C.READ_TOKEN_TTL_S)))
            pos += m["size"]
        return f, out
