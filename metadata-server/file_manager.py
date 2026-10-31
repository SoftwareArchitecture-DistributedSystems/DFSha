"""Sesiones de escritura (open/commit/close) y locks con lease."""
import time, uuid

from shared import constants as C
from shared.exceptions import DFSError, INVALID, NOT_FOUND


class FileManager:
    def __init__(self, repo):
        self.repo = repo

    def open(self, path, uid, mode, holder):
        if mode not in ("w", "a"):
            raise DFSError(INVALID, "modo de escritura: w (crear/reemplazar) o a (anexar)")
        # el sid lo genera el líder y viaja a Raft ya armado; si no, cada réplica
        # generaría un uuid distinto y el estado divergiría.
        sid = str(uuid.uuid4())
        r = self.repo.raft("open", path=path, uid=uid, holder=holder or uid, mode=mode, sid=sid, now=time.time())
        return sid, r

    def session(self, sid, uid):
        p = self.repo.pending.get(sid)
        if not p or p["uid"] != uid:
            raise DFSError(NOT_FOUND, "sesión de escritura inexistente")
        return p

    def commit_block(self, sid, uid, req):
        # todavía no publica nada: solo anota el bloque en la sesión abierta
        self.repo.raft("commit", sid=sid, uid=uid, idx=req.block_index, block_id=req.block_id, version=req.version,
                       sha=req.sha256, size=req.size, replicas=list(req.replicas),
                       expected=req.expected_version, now=time.time())

    def commit_file(self, sid, uid, req):
        # acá se hace el compare-and-swap contra la versión que el cliente abrió
        r = self.repo.raft("close", sid=sid, uid=uid, expected=req.expected_version, n=req.num_blocks,
                           total=req.total_size, tid=str(uuid.uuid4()), now=time.time())
        return r["version"]

    def acquire_lock(self, path, idx, uid, holder, mode, ttl):
        if path not in self.repo.files:
            raise DFSError(NOT_FOUND, path)
        # S = compartido, X = exclusivo. Cualquier otra cosa la tratamos como exclusivo.
        m = "S" if mode.upper().startswith("S") else "X"
        r = self.repo.raft("lock", path=path, idx=idx, holder=holder or uid, mode=m,
                           ttl=min(ttl or C.LOCK_TTL_S, 120), now=time.time())
        return r["exp"]

    def release_lock(self, path, idx, uid, holder):
        self.repo.raft("unlock", path=path, idx=idx, holder=holder or uid)

    def locked_by_other(self, path, idx, holder):
        # nos fijan si hay algún lock exclusivo vigente de otro cliente, ya sea
        # sobre el bloque puntual o sobre el archivo completo (-1).
        now = time.time()
        for k in self.repo.lock_keys(path, idx):
            for h, (m, exp) in self.repo.locks.get(k, {}).items():
                if h != holder and m == "X" and exp > now:
                    return True
        return False
