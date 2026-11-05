"""Cliente gRPC del plano de control. Usa LeaderRouter y convierte los Status en DFSError."""
import auth_pb2 as apb
import auth_pb2_grpc as arpc
import metadata_pb2 as mpb
import metadata_pb2_grpc as mrpc
from shared.exceptions import DFSError
from shared.utils import LeaderRouter


def _ok(resp):
    # el MS siempre responde con un Status; si no es OK, lo levantamos como excepción
    if resp.status.code != "OK":
        raise DFSError(resp.status.code, resp.status.message)
    return resp


class MetadataClient:
    def __init__(self, seeds, session):
        self.router, self.session = LeaderRouter(seeds), session

    def _meta(self, method, req): return _ok(self.router.call(mrpc.MetadataServiceStub, method, req))
    def _auth(self, method, req): return _ok(self.router.call(arpc.AuthServiceStub, method, req))

    @property
    def jwt(self):
        # se lee de la sesión en cada llamada para que login() no tenga que recrear el cliente
        return self.session.data["jwt"]

    def register(self, uid, pw): self._auth("Register", apb.Credentials(uid=uid, password=pw))
    def authenticate(self, uid, pw): return self._auth("Authenticate", apb.Credentials(uid=uid, password=pw)).jwt

    # Namespace
    def mkdir(self, path): self._meta("Mkdir", mpb.PathRequest(jwt=self.jwt, path=path))
    def rmdir(self, path): self._meta("Rmdir", mpb.PathRequest(jwt=self.jwt, path=path))
    def delete_file(self, path): self._meta("DeleteFile", mpb.PathRequest(jwt=self.jwt, path=path))
    def list_dir(self, path): return self._meta("ListDir", mpb.PathRequest(jwt=self.jwt, path=path)).entries

    # Escritura: abrir sesión, pedir bloques y confirmar
    def open_file(self, path, mode, holder):
        return self._meta("OpenFile", mpb.OpenRequest(jwt=self.jwt, path=path, mode=mode, holder=holder))

    def allocate_block(self, session, idx):
        return self._meta("AllocateBlock", mpb.AllocateRequest(jwt=self.jwt, session=session, block_index=idx))

    def commit_block(self, session, idx, alloc, sha, size, replicas, expected_version):
        self._meta("CommitBlock", mpb.CommitBlockRequest(
            jwt=self.jwt, session=session, block_index=idx, block_id=alloc.block_id, version=alloc.version,
            sha256=sha, size=size, replicas=replicas, expected_version=expected_version))

    def commit_file(self, session, expected_version, num_blocks, total_size):
        return self._meta("CommitFile", mpb.CommitFileRequest(
            jwt=self.jwt, session=session, expected_version=expected_version,
            num_blocks=num_blocks, total_size=total_size)).file_version

    # Lectura
    def get_block_locations(self, path, offset=0, length=0):
        return self._meta("GetBlockLocations", mpb.LocationsRequest(jwt=self.jwt, path=path, offset=offset, length=length))

    # Locks
    def acquire_lock(self, path, idx, mode, holder, ttl=0):
        return self._meta("AcquireLock", mpb.LockRequest(jwt=self.jwt, path=path, block_index=idx, mode=mode,
                                                         ttl=ttl, holder=holder)).expires_at

    def release_lock(self, path, idx, holder):
        self._meta("ReleaseLock", mpb.LockRequest(jwt=self.jwt, path=path, block_index=idx, holder=holder))
