"""Verificación de integridad: SHA-256 por bloque y scrubbing periódico."""
import logging, time

from shared import config
from shared.utils import sha256_hex

log = logging.getLogger("dn.integrity")


def verify_hash(data: bytes, expected: str) -> bool:
    # comparación directa: acá no hay secreto que filtrar por timing
    return sha256_hex(data) == expected


def scrub(storage):
    """Descifra todos los bloques. El tag GCM detecta alteraciones.

    Si un bloque está corrupto lo borramos. En el próximo BlockReport el MS ve que
    falta esa réplica y la vuelve a copiar desde otro nodo.
    """
    corrupt = []
    for bid, _ in storage.list_blocks():
        try:
            storage.load(bid)
        except Exception:
            log.error("bloque corrupto %s: se elimina", bid)
            storage.delete(bid)
            corrupt.append(bid)
    return corrupt


def scrub_loop(storage):
    while True:
        time.sleep(config.SCRUB_INTERVAL_S)
        scrub(storage)
