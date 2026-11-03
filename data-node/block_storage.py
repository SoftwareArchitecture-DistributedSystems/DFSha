"""Almacenamiento local de bloques cifrados: [versión u32][nonce 12B][ciphertext+tag] (AES-256-GCM)."""
import glob, os, shutil, struct, threading, time

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from shared.security import derive_block_key


class BlockStorage:
    def __init__(self, directory: str, master_key: bytes):
        self.dir, self.master = directory, master_key
        os.makedirs(directory, exist_ok=True)
        # Si el proceso murió a mitad de una escritura, quedan .tmp que no sirven.
        for tmp in glob.glob(f"{directory}/*.tmp"):
            os.remove(tmp)

    def _path(self, bid):
        return f"{self.dir}/{bid}.blk"

    def store(self, bid: str, version: int, data: bytes):
        """Escribe en .tmp, hace fsync y renombra. Así un bloque a medias nunca queda visible."""
        nonce = os.urandom(12)
        aead = AESGCM(derive_block_key(self.master, bid, version))
        # Formato en disco: versión (4 bytes) + nonce (12) + ciphertext con tag GCM.
        blob = struct.pack(">I", version) + nonce + aead.encrypt(nonce, data, bid.encode())
        tmp = f"{self.dir}/{bid}.{os.getpid()}.{threading.get_ident()}.tmp"
        with open(tmp, "wb") as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
        # replace es atómico dentro del mismo filesystem
        os.replace(tmp, self._path(bid))

    def load(self, bid: str):
        with open(self._path(bid), "rb") as f:
            blob = f.read()
        version = struct.unpack(">I", blob[:4])[0]
        aead = AESGCM(derive_block_key(self.master, bid, version))
        # El block_id se usa como datos asociados: si alguien mueve el archivo de lugar,
        # el descifrado falla.
        return version, aead.decrypt(blob[4:16], blob[16:], bid.encode())

    def exists(self, bid):
        return os.path.exists(self._path(bid))

    def delete(self, bid) -> bool:
        if self.exists(bid):
            os.remove(self._path(bid))
            return True
        return False

    def list_blocks(self):
        # devolvemos (block_id, antigüedad en segundos); la antigüedad la usa el MS
        # para decidir si un bloque huérfano ya se puede borrar.
        now = time.time()
        return [(os.path.basename(p)[:-4], int(now - os.path.getmtime(p))) for p in glob.glob(f"{self.dir}/*.blk")]

    def free_bytes(self):
        return shutil.disk_usage(self.dir).free
