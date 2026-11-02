"""Servicios gRPC de metadatos y autenticación. Solo el líder atiende; los demás responden NOT_LEADER."""
import logging, posixpath

import auth_pb2 as apb
import auth_pb2_grpc as arpc
import metadata_pb2 as mpb
import metadata_pb2_grpc as mrpc
from shared import constants as C
from shared.exceptions import DFSError, UNAVAILABLE
from shared.utils import normalize_path

log = logging.getLogger("ms.api")
QUIET = ("Heartbeat", "ListDir", "GetBlockLocations", "BlockReport")


def api(resp_cls, auth=True):
    """Envuelve un método gRPC: primero líder, después JWT, después la lógica.

    La auditoría se hace acá para no repetir logs en cada método. Los heartbeats y
    los listados se saltan el log si salieron bien, porque ensucian mucho.
    """
    def deco(fn):
        def wrap(self, req, ctx):
            if not self.repo.is_leader():
                # el cliente usa leader para saber a quién preguntar después
                return resp_cls(status=apb.Status(code="NOT_LEADER", leader=self.repo.leader_hint(C.MS_GRPC_OFFSET)))
            uid, code = "-", "OK"
            try:
                uid = self.users.uid_from_token(req.jwt) if auth else "-"
                out = fn(self, req, uid)
                out.status.code = "OK"
                return out
            except DFSError as e:
                # errores de negocio: se devuelven tal cual, sin stacktrace
                code = e.code
                return resp_cls(status=apb.Status(code=e.code, message=e.message))
            except Exception as e:
                # cualquier otra cosa es un bug o una caída; la logueamos y devolvemos UNAVAILABLE
                log.exception("fallo interno")
                code = UNAVAILABLE
                return resp_cls(status=apb.Status(code=UNAVAILABLE, message=str(e)))
            finally:
                if fn.__name__ not in QUIET or code != "OK":
                    log.info("AUDIT op=%s uid=%s code=%s", fn.__name__, uid, code)
        wrap.__name__ = fn.__name__
        return wrap
    return deco


class AuthService(arpc.AuthServiceServicer):
    def __init__(self, repo, users):
        self.repo, self.users = repo, users

    @api(apb.AuthResponse, auth=False)
    def Register(self, req, _):
        # el hash de la contraseña se calcula antes de entrar a Raft, no dentro
        self.users.register(req.uid, req.password)
        return apb.AuthResponse()

    @api(apb.AuthResponse, auth=False)
    def Authenticate(self, req, _):
        token, exp = self.users.authenticate(req.uid, req.password)
        return apb.AuthResponse(jwt=token, expires_at=exp)


class MetadataService(mrpc.MetadataServiceServicer):
    def __init__(self, repo, users, namespace, files, blocks, registry, replication):
        # Hay muchos colaboradores, pero cada uno tiene una responsabilidad bastante clara.
        self.repo, self.users, self.ns, self.files = repo, users, namespace, files
        self.blocks, self.registry, self.replication = blocks, registry, replication

    def _own(self, path, uid):
        # atajo para no repetir la validación de ACL en cada método
        return self.users.authorize_path(path, uid)

    # Espacio de nombres
    @api(mpb.Ack)
    def Mkdir(self, req, uid):
        self.ns.mkdir(self._own(req.path, uid), uid)
        return mpb.Ack()

    @api(mpb.Ack)
    def Rmdir(self, req, uid):
        self.ns.rmdir(self._own(req.path, uid), uid)
        return mpb.Ack()

    @api(mpb.Ack)
    def DeleteFile(self, req, uid):
        self.ns.rm(self._own(req.path, uid), uid)
        return mpb.Ack()

    @api(mpb.ListResponse)
    def ListDir(self, req, uid):
        entries = self.ns.list(self._own(req.path or f"/users/{uid}", uid), uid)
        return mpb.ListResponse(entries=[mpb.Entry(name=posixpath.basename(k), is_dir=v["type"] == "dir",
                                                   size=v.get("size", 0), version=v.get("version", 0))
                                         for k, v in entries])

    # Escritura. El cliente abre una sesión, pide bloques y al final confirma el archivo.
    @api(mpb.OpenResponse)
    def OpenFile(self, req, uid):
        sid, r = self.files.open(self._own(req.path, uid), uid, req.mode or "w", req.holder)
        return mpb.OpenResponse(session=sid, file_version=r["base"], num_blocks=r["n"], file_size=r["size"])

    @api(mpb.AllocateResponse)
    def AllocateBlock(self, req, uid):
        bid, version, pipe, token = self.blocks.allocate(req.session, req.block_index, uid)
        return mpb.AllocateResponse(block_id=bid, version=version, pipeline=pipe, write_token=token)

    @api(mpb.Ack)
    def CommitBlock(self, req, uid):
        self.files.commit_block(req.session, uid, req)
        return mpb.Ack()

    @api(mpb.CommitFileResponse)
    def CommitFile(self, req, uid):
        return mpb.CommitFileResponse(file_version=self.files.commit_file(req.session, uid, req))

    # Lectura: el MS devuelve dónde están las réplicas y un token para cada bloque.
    @api(mpb.LocationsResponse)
    def GetBlockLocations(self, req, uid):
        f, locs = self.blocks.locations(self._own(req.path, uid), uid, req.offset, req.length)
        return mpb.LocationsResponse(file_version=f["version"], total_size=f["size"], blocks=[
            mpb.BlockLocation(index=i, block_id=bid, version=m["version"], sha256=m["sha"], size=m["size"],
                              offset=pos, replicas=reps, read_token=tok) for i, bid, m, pos, reps, tok in locs])

    # Locks. block_index = -1 significa el archivo completo.
    @api(mpb.LockResponse)
    def AcquireLock(self, req, uid):
        exp = self.files.acquire_lock(self._own(req.path, uid), req.block_index, uid, req.holder, req.mode, req.ttl)
        return mpb.LockResponse(expires_at=exp)

    @api(mpb.Ack)
    def ReleaseLock(self, req, uid):
        self.files.release_lock(self._own(req.path, uid), req.block_index, uid, req.holder)
        return mpb.Ack()

    # Plano DN -> MS. No lleva JWT porque estos nodos se autentican con mTLS.
    @api(mpb.Ack, auth=False)
    def Heartbeat(self, req, _):
        self.registry.heartbeat(req.addr, req.free_bytes)
        return mpb.Ack()

    @api(mpb.Ack, auth=False)
    def BlockReport(self, req, _):
        self.replication.reconcile(req.addr, [(b.block_id, b.age_s) for b in req.blocks])
        return mpb.Ack()
