import hashlib, logging, posixpath, time

import grpc

from shared import constants as C
from shared.exceptions import DFSError, UNAVAILABLE
from shared.security import open_channel, server_credentials


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def split_chunks(data: bytes, size: int = C.CHUNK_SIZE):
    # max(len(data), 1) hace que un archivo vacío igual genere un chunk vacío
    for i in range(0, max(len(data), 1), size):
        yield data[i:i + size]


def normalize_path(path: str) -> str:
    # posixpath y no os.path: las rutas del DFS son siempre con "/"
    return posixpath.normpath(path)


def setup_logging(level=logging.INFO):
    logging.basicConfig(level=level, format="%(asctime)s %(name)s %(message)s")


def start_grpc_server(server, port: int):
    creds = server_credentials()
    if creds:
        server.add_secure_port(f"0.0.0.0:{port}", creds)
    else:
        # modo desarrollo: cualquiera que llegue al puerto puede hablar con el servicio
        server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()


class LeaderRouter:
    """Transparencia de localización: arranca con una lista semilla y sigue el NOT_LEADER."""

    def __init__(self, seeds):
        self.seeds, self.leader, self._stubs = list(seeds), None, {}

    def stub(self, cls, addr):
        # cacheamos un stub por dirección para no abrir un canal nuevo en cada llamada
        if (cls, addr) not in self._stubs:
            self._stubs[(cls, addr)] = cls(open_channel(addr))
        return self._stubs[(cls, addr)]

    def call(self, stub_cls, method, req, timeout=15, rounds=6):
        for _ in range(rounds):
            # primero probamos el líder que conocemos y después las semillas
            queue = ([self.leader] if self.leader else []) + self.seeds
            tried = set()
            while queue:
                addr = queue.pop(0)
                if addr in tried:
                    continue
                tried.add(addr)
                try:
                    resp = getattr(self.stub(stub_cls, addr), method)(req, timeout=timeout)
                except grpc.RpcError:
                    # nodo caído o inaccesible: pasamos al siguiente
                    continue
                if resp.status.code == "NOT_LEADER":
                    if resp.status.leader:
                        # el MS nos dijo quién es el líder; lo probamos primero
                        queue.insert(0, resp.status.leader)
                    continue
                self.leader = addr
                return resp
            # nos quedamos sin candidatos: puede que haya una elección en curso
            self.leader = None
            time.sleep(1.5)
        raise DFSError(UNAVAILABLE, "ningún servidor de metadatos respondió")
