"""Estado lógico replicado con Raft (PySyncObj): namespace, usuarios, archivos, bloques, sesiones y locks.

Toda mutación pasa por do(op, args), que Raft aplica en el mismo orden en los tres MS. Por eso lo no
determinista (uuid, hora, hash de contraseña) lo calcula el líder y llega ya resuelto en `a`.
"""
import os, posixpath

from pysyncobj import SyncObj, SyncObjConf, SyncObjException, replicated

from shared import config
from shared import constants as C
from shared.exceptions import (ALREADY_EXISTS, BLOCK_LOCKED, DFSError, INCOMPLETE, INSUFFICIENT_REPLICAS,
                               INVALID, NOT_EMPTY, NOT_FOUND, NO_QUORUM, PERMISSION_DENIED, VERSION_CONFLICT)


# Las operaciones replicadas devuelven una tupla simple en vez de levantar excepciones:
# las excepciones no viajan bien a través de Raft.
def ERR(code, msg=""): return ("ERR", code, msg)
def OK(data=None): return ("OK", data)


class MetadataRepository(SyncObj):
    def __init__(self, self_addr, peers, data_dir):
        os.makedirs(data_dir, exist_ok=True)
        cfg = SyncObjConf(dynamicMembershipChange=False, fullDumpFile=f"{data_dir}/raft.dump",
                          journalFile=f"{data_dir}/raft.journal", password=config.RAFT_PASSWORD,
                          raftMinTimeout=1.0, raftMaxTimeout=2.0)
        super().__init__(self_addr, peers, cfg)
        # Todo esto se replica por Raft. Ojo: no guardar acá cosas que cambien solas
        # (por ejemplo time.time()) fuera de una operación.
        self.users = {}      # uid -> hash argon2
        self.files = {"/": {"type": "dir"}, "/users": {"type": "dir"}}
        self.blocks = {}     # block_id -> {version, sha, size, replicas}
        self.pending = {}    # session -> escritura abierta (todavía no publicada)
        self.locks = {}      # "ruta:idx" -> {holder: (modo, expira)}
        self.tombs = []      # borrado lógico: {id, ts, blocks}

    # API que usan los managers
    def is_leader(self): return self._isLeader()

    def leader_hint(self, grpc_offset):
        leader = self._getLeader()
        if not leader:
            return ""
        host, port = str(getattr(leader, "id", leader)).rsplit(":", 1)
        return f"{host}:{int(port) + grpc_offset}"

    def raft(self, op, **a):
        """Replica la operación por mayoría. Devuelve el resultado o levanta DFSError."""
        try:
            r = self.do(op, a, sync=True, timeout=5)
        except SyncObjException as e:
            # nos quedamos sin quórum: el cliente puede reintentar más tarde
            raise DFSError(NO_QUORUM, f"sin mayoría Raft: {e}")
        if r[0] == "ERR":
            raise DFSError(r[1], r[2])
        return r[1]

    @replicated
    def do(self, op, a):
        # el nombre de la operación se usa para buscar el método _op_<nombre>
        return getattr(self, "_op_" + op)(a)

    # Usuarios y namespace
    def _op_register(self, a):
        if a["uid"] in self.users:
            return ERR(ALREADY_EXISTS, "usuario ya existe")
        self.users[a["uid"]] = a["pw"]
        # cada usuario arranca con su home creado
        self.files[f"/users/{a['uid']}"] = {"type": "dir", "owner": a["uid"]}
        return OK()

    def _op_mkdir(self, a):
        p = a["path"]
        if p in self.files:
            return ERR(ALREADY_EXISTS, p)
        parent = self.files.get(posixpath.dirname(p))
        if not parent or parent["type"] != "dir":
            return ERR(NOT_FOUND, "directorio padre no existe")
        self.files[p] = {"type": "dir", "owner": a["uid"]}
        return OK()

    def _op_remove(self, a):
        p, f = a["path"], self.files.get(a["path"])
        if not f:
            return ERR(NOT_FOUND, p)
        if f["type"] == "dir":
            if a["kind"] != "dir":
                return ERR(INVALID, "es un directorio: use rmdir")
            # rmdir es como en Unix: no borra si hay algo adentro
            if any(k.startswith(p + "/") for k in self.files):
                return ERR(NOT_EMPTY, "rmdir solo elimina directorios vacíos")
            del self.files[p]
        else:
            if a["kind"] != "file":
                return ERR(INVALID, "es un archivo: use rm")
            # No borramos los bloques en el acto: los dejamos en la lápida y el
            # recolector los limpia después del periodo de gracia.
            del self.files[p]
            self.tombs.append({"id": a["tid"], "ts": a["now"], "blocks": f["blocks"]})
        return OK()

    # Escritura: open, después uno o más commit y al final close. Nada se publica hasta close.
    def _op_open(self, a):
        f = self.files.get(a["path"])
        if f and f["type"] == "dir":
            return ERR(INVALID, "es un directorio")
        parent = self.files.get(posixpath.dirname(a["path"]))
        if not parent or parent["type"] != "dir":
            return ERR(NOT_FOUND, "directorio padre no existe")
        if f and f["owner"] != a["uid"]:
            return ERR(PERMISSION_DENIED, a["path"])
        if a["mode"] == "a" and not f:
            return ERR(NOT_FOUND, "no se puede anexar a un archivo inexistente")

        # En modo append arrancamos con los bloques que ya existen. En modo w
        # empezamos de cero y los bloques viejos se tiran al publicar.
        order = {i: b for i, b in enumerate(f["blocks"])} if (f and a["mode"] == "a") else {}
        base = f["version"] if f else 0
        self.pending[a["sid"]] = {"path": a["path"], "uid": a["uid"], "holder": a["holder"], "ts": a["now"],
                                  "base": base, "order": order, "new": {}}
        return OK({"base": base, "n": len(order), "size": f["size"] if (f and a["mode"] == "a") else 0})

    def _cur_version(self, path):
        f = self.files.get(path)
        return f["version"] if f else 0

    def _op_commit(self, a):
        p = self.pending.get(a["sid"])
        if not p or p["uid"] != a["uid"]:
            return ERR(NOT_FOUND, "sesión de escritura inexistente")
        # si otro escritor publicó mientras estábamos subiendo, abortamos
        if self._cur_version(p["path"]) != p["base"] or a["expected"] != p["base"]:
            return ERR(VERSION_CONFLICT, "la versión vigente cambió")
        if len(a["replicas"]) < C.WRITE_QUORUM:
            return ERR(INSUFFICIENT_REPLICAS, f"se requieren {C.WRITE_QUORUM} ACK")

        # idempotente: si el cliente reintenta el mismo bloque, se pisa con lo mismo
        p["order"][a["idx"]] = a["block_id"]
        p["new"][a["block_id"]] = {"version": a["version"], "sha": a["sha"], "size": a["size"],
                                   "replicas": list(a["replicas"])}
        p["ts"] = a["now"]
        return OK()

    def _op_close(self, a):
        p = self.pending.get(a["sid"])
        if not p or p["uid"] != a["uid"]:
            return ERR(NOT_FOUND, "sesión de escritura inexistente")
        cur = self.files.get(p["path"])

        # compare-and-swap por versión: solo publica el primero que llega
        if self._cur_version(p["path"]) != p["base"] or a["expected"] != p["base"]:
            return ERR(VERSION_CONFLICT, "otro escritor publicó primero")

        n, order = a["n"], p["order"]
        if sorted(order) != list(range(n)):
            return ERR(INCOMPLETE, "faltan bloques por confirmar")
        ids = [order[i] for i in range(n)]

        # el tamaño total tiene que coincidir con la suma de los bloques confirmados
        total = sum((p["new"].get(b) or self.blocks[b])["size"] for b in ids)
        if total != a["total"]:
            return ERR(INCOMPLETE, "la suma de tamaños no coincide")

        # bloques de la versión anterior y bloques nuevos que no quedaron en el archivo
        garbage = [b for b in (cur["blocks"] if cur else []) if b not in ids] + [b for b in p["new"] if b not in ids]
        self.blocks.update(p["new"])
        if garbage:
            self.tombs.append({"id": a["tid"], "ts": a["now"], "blocks": garbage})
        self.files[p["path"]] = {"type": "file", "owner": p["uid"], "version": p["base"] + 1,
                                 "size": total, "blocks": ids}
        del self.pending[a["sid"]]
        return OK({"version": p["base"] + 1})

    def _op_expire(self, a):
        # escritura abandonada: los bloques que se llegaron a subir se borran después
        p = self.pending.pop(a["sid"], None)
        if p and p["new"]:
            self.blocks.update(p["new"])
            self.tombs.append({"id": a["tid"], "ts": a["now"], "blocks": list(p["new"])})
        return OK()

    # Locks con vencimiento. El cliente tiene que renovarlos antes de que expiren.
    def lock_keys(self, path, idx):
        # Un lock sobre el archivo (-1) choca con cualquier lock de bloque; por eso
        # al consultar siempre miramos también la clave del archivo completo.
        if idx == -1:
            return [k for k in self.locks if k.startswith(path + ":")] + [f"{path}:-1"]
        return [f"{path}:{idx}", f"{path}:-1"]

    def _op_lock(self, a):
        now, holder, mode = a["now"], a["holder"], a["mode"]
        for k in self.lock_keys(a["path"], a["idx"]):
            for h, (m, exp) in self.locks.get(k, {}).items():
                # dos lectores (S) pueden convivir; cualquier cosa con X choca
                if h != holder and exp > now and (mode == "X" or m == "X"):
                    return ERR(BLOCK_LOCKED, f"bloqueado por otro cliente ({k})")
        key = f"{a['path']}:{a['idx']}"
        # limpiamos los vencidos antes de guardar el nuevo
        cur = {h: v for h, v in self.locks.get(key, {}).items() if v[1] > now}
        cur[holder] = (mode, now + a["ttl"])
        self.locks[key] = cur
        return OK({"exp": int(now + a["ttl"])})

    def _op_unlock(self, a):
        self.locks.get(f"{a['path']}:{a['idx']}", {}).pop(a["holder"], None)
        return OK()

    # Réplicas y recolección de basura
    def _op_add_replica(self, a):
        # es idempotente: si el DN ya estaba, no lo duplicamos
        m = self.blocks.get(a["block_id"])
        if m and a["addr"] not in m["replicas"]:
            m["replicas"].append(a["addr"])
        return OK()

    def _op_drop_replica(self, a):
        m = self.blocks.get(a["block_id"])
        if m and a["addr"] in m["replicas"]:
            m["replicas"].remove(a["addr"])
        return OK()

    def _op_gc(self, a):
        # cierra la lápida: recién acá desaparecen los metadatos de los bloques borrados
        for t in [t for t in self.tombs if t["id"] == a["tid"]]:
            for b in t["blocks"]:
                self.blocks.pop(b, None)
            self.tombs.remove(t)
        return OK()
