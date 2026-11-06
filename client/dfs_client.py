"""Biblioteca de alto nivel de DFSha. Cubre el namespace, put/get y los handles de RF3."""
import json, os, posixpath, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor

from data_node_client import DataNodeClient
from metadata_client import MetadataClient
from shared import config, constants as C
from shared.exceptions import DATA_LOSS, DFSError, INVALID, UNAVAILABLE
from shared.utils import sha256_hex


class Session:
    """Estado local del cliente: JWT, usuario, directorio actual e identidad para los locks."""

    def __init__(self, path=None, client_id=None):
        self.path = path or config.SESSION_FILE
        self.data = {"jwt": "", "uid": "", "cwd": "/", "cid": ""}
        # si ya hay una sesión guardada, la reutilizamos (por ejemplo para conservar el cwd)
        if os.path.exists(self.path):
            self.data.update(json.load(open(self.path)))
        # el cid identifica a este cliente frente a los locks; si no nos dan uno, lo inventamos
        self.data["cid"] = client_id or os.environ.get("DFSHA_CLIENT_ID") or self.data["cid"] or uuid.uuid4().hex[:8]

    def save(self):
        with open(self.path, "w") as f:
            json.dump(self.data, f)
        # el archivo tiene el JWT, así que lo dejamos solo para el usuario
        os.chmod(self.path, 0o600)


class DFSClient:
    def __init__(self, seeds=None, session_file=None, client_id=None):
        self.session = Session(session_file, client_id)
        self.meta = MetadataClient(seeds or config.DEFAULT_MS.split(","), self.session)
        self.data = DataNodeClient()

    @property
    def cid(self): return self.session.data["cid"]

    def abspath(self, p):
        # las rutas relativas se resuelven contra el cwd que guarda la sesión
        return posixpath.normpath(p if p.startswith("/") else posixpath.join(self.session.data["cwd"], p))

    def register(self, uid, password):
        self.meta.register(uid, password)

    def login(self, uid, password):
        # al entrar arrancamos parados en el home del usuario
        self.session.data.update(jwt=self.meta.authenticate(uid, password), uid=uid, cwd=f"/users/{uid}")
        self.session.save()

    def pwd(self):
        return self.session.data["cwd"]

    def cd(self, path):
        self.session.data["cwd"] = self.abspath(path)
        self.session.save()

    # operaciones de namespace: son finas, el trabajo real está en el MS
    def ls(self, path=None): return self.meta.list_dir(self.abspath(path or self.pwd()))
    def mkdir(self, path): self.meta.mkdir(self.abspath(path))
    def rmdir(self, path): self.meta.rmdir(self.abspath(path))
    def rm(self, path): self.meta.delete_file(self.abspath(path))
    def lock(self, path, block=-1, mode="X"): return self.meta.acquire_lock(self.abspath(path), block, mode, self.cid)
    def unlock(self, path, block=-1): self.meta.release_lock(self.abspath(path), block, self.cid)

    def open(self, path, mode="r"):
        return FileHandle(self, self.abspath(path), mode)

    # put/get son la vía simple para archivos completos; por dentro usan un FileHandle
    def put(self, local, remote, mode="w"):
        t0, size = time.time(), os.path.getsize(local)
        with self.open(remote, mode) as h, open(local, "rb") as f:
            while True:
                data = f.read(C.BLOCK_SIZE)
                if not data:
                    break
                h.write(data)
        return size, time.time() - t0

    def get(self, remote, local):
        t0, n = time.time(), 0
        with self.open(remote, "r") as h, open(local, "wb") as f:
            for data in h.iter_blocks():
                f.write(data)
                n += len(data)
        return n, time.time() - t0


class FileHandle:
    """Handle de open(): r lee la versión que estaba vigente al abrir, w crea o reemplaza, a anexa."""

    def __init__(self, client, path, mode):
        if mode not in ("r", "w", "a"):
            raise DFSError(INVALID, "modo: r, w o a")
        self.c, self.path, self.mode, self.closed = client, path, mode, False
        self._keeper = None
        if mode == "r":
            # guardamos la lista de bloques al abrir: si otro escribe después, seguimos leyendo esta versión
            self.loc = client.meta.get_block_locations(path)
            self.size, self.version, self.pos = self.loc.total_size, self.loc.file_version, 0
        else:
            if mode == "a":
                # para anexar necesitamos el lock exclusivo del archivo completo (-1)
                client.meta.acquire_lock(path, -1, "X", client.cid)
                self._start_lease_renewal()
            try:
                o = client.meta.open_file(path, mode, client.cid)
            except DFSError:
                # si el MS rechaza la apertura, no dejamos el lock colgado
                self._release_lock()
                raise
            self.session, self.base = o.session, o.file_version
            self.idx, self.total, self.buf = o.num_blocks, o.file_size, bytearray()

    def _start_lease_renewal(self):
        # el lease vence a los 30 s, así que lo renovamos cada 10 s mientras el handle esté abierto
        stop = self._stop = threading.Event()

        def renew():
            while not stop.wait(C.LOCK_RENEW_S):
                try:
                    self.c.meta.acquire_lock(self.path, -1, "X", self.c.cid)
                except DFSError:
                    # si no se pudo renovar, el lock ya se perdió; no insiste
                    return
        self._keeper = threading.Thread(target=renew, daemon=True)
        self._keeper.start()

    def _release_lock(self):
        if self._keeper:
            self._stop.set()
            try:
                self.c.meta.release_lock(self.path, -1, self.c.cid)
            except DFSError:
                # si el MS ya lo dio por vencido, no pasa nada
                pass
            self._keeper = None

    def _fetch(self, loc):
        data = self.c.data.read_block(loc)
        if data is None:
            # el DN cliente ya probó todas las réplicas. Puede que haya cambiado la ubicación,
            # así que preguntamos de nuevo al MS (solo si seguimos en la misma versión).
            fresh = self.c.meta.get_block_locations(self.path, loc.offset, 1)
            if fresh.file_version == self.version and fresh.blocks:
                data = self.c.data.read_block(fresh.blocks[0])
        if data is None:
            raise DFSError(DATA_LOSS, f"bloque {loc.index} ilegible en todas las réplicas")
        return data

    def iter_blocks(self, workers=8):
        """Baja los bloques en paralelo de a `workers` y los devuelve en orden."""
        blocks = list(self.loc.blocks)
        with ThreadPoolExecutor(workers) as ex:
            # map respeta el orden de la lista, aunque las descargas terminen desordenadas
            for i in range(0, len(blocks), workers):
                yield from ex.map(self._fetch, blocks[i:i + workers])

    def seek(self, pos):
        self.pos = pos

    def read(self, size=-1):
        if self.mode != "r":
            raise DFSError(INVALID, "handle abierto para escritura")
        end = self.size if size < 0 else min(self.size, self.pos + size)
        if end <= self.pos:
            return b""
        # nos quedamos solo con los bloques que tocan el rango pedido
        locs = [b for b in self.loc.blocks if b.offset < end and b.offset + b.size > self.pos]
        with ThreadPoolExecutor(8) as ex:
            data = b"".join(ex.map(self._fetch, locs))
        # recortamos el primer y el último bloque si el rango empieza o termina en el medio
        start = self.pos - locs[0].offset
        out = data[start:start + (end - self.pos)]
        self.pos = end
        return out

    # La escritura es copy-on-write: cada bloque que se manda es nuevo y la versión
    # anterior sigue visible hasta que close() publica el archivo completo.
    def write(self, data: bytes):
        if self.mode == "r":
            raise DFSError(INVALID, "handle abierto para lectura")
        self.buf += data
        # vamos vaciando de a bloques completos; el resto queda en el buffer para close()
        while len(self.buf) >= C.BLOCK_SIZE:
            self._flush(bytes(self.buf[:C.BLOCK_SIZE]))
            del self.buf[:C.BLOCK_SIZE]

    def _flush(self, data):
        sha = sha256_hex(data)
        # Si el líder acaba de cambiar, puede que todavía no tenga heartbeats de los DN.
        # Reintentamos un par de veces antes de rendirnos.
        for attempt in range(4):
            try:
                alloc = self.c.meta.allocate_block(self.session, self.idx)
                acked = self.c.data.write_block(alloc, data, sha)
            except DFSError as e:
                if e.code != "INSUFFICIENT_REPLICAS":
                    raise
                time.sleep(3)
                continue
            self.c.meta.commit_block(self.session, self.idx, alloc, sha, len(data), acked, self.base)
            self.idx += 1
            self.total += len(data)
            return
        raise DFSError("INSUFFICIENT_REPLICAS", f"no se pudo escribir el bloque {self.idx}")

    def close(self):
        """Manda los bloques que quedaron en el buffer y publica la versión nueva."""
        if self.closed:
            return self.version if self.mode == "r" else getattr(self, "new_version", None)
        self.closed = True
        if self.mode == "r":
            return self.version
        try:
            if self.buf:
                self._flush(bytes(self.buf))
                self.buf.clear()
            # el MS hace compare-and-swap contra self.base; si otro publicó antes, esto falla
            self.new_version = self.c.meta.commit_file(self.session, self.base, self.idx, self.total)
            return self.new_version
        finally:
            self._release_lock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type:
            # hubo una excepción: no publicamos nada y dejamos que la sesión expire
            self.closed = True
            self._release_lock()
        else:
            self.close()
