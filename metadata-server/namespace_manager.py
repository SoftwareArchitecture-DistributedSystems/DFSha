"""Espacio de nombres: directorios y listados."""
import posixpath
import time, uuid

from shared.exceptions import DFSError, NOT_FOUND, PERMISSION_DENIED


class NamespaceManager:
    def __init__(self, repo):
        self.repo = repo

    def mkdir(self, path, uid):
        # el uuid y la hora se generan fuera de Raft para que la operación sea determinista
        self.repo.raft("mkdir", path=path, uid=uid)

    def _remove(self, path, uid, kind):
        if path == f"/users/{uid}":
            raise DFSError(PERMISSION_DENIED, "no puedes borrar tu directorio raíz")
        # kind distingue rm de rmdir; el tid sirve para agrupar el borrado diferido
        self.repo.raft("remove", path=path, uid=uid, kind=kind, tid=str(uuid.uuid4()), now=time.time())

    def rmdir(self, path, uid): self._remove(path, uid, "dir")
    def rm(self, path, uid): self._remove(path, uid, "file")

    def list(self, path, uid):
        f = self.repo.files.get(path)
        if not f:
            raise DFSError(NOT_FOUND, path)
        if f["type"] != "dir":
            # listar un archivo devuelve una sola entrada; es cómodo para el cliente
            return [(path, f)]
        # sorted para que el listado sea estable entre réplicas y llamadas
        return [(k, v) for k, v in sorted(self.repo.files.items()) if posixpath.dirname(k) == path and k != path]
