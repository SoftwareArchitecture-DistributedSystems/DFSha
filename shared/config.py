"""Configuración por variables de entorno. Los valores por defecto son solo para desarrollo."""
import os


def _int(name, default):
    # las variables de entorno vienen como texto, así que hay que convertirlas
    return int(os.environ.get(name, default))


# Si está vacío se trabaja sin TLS. En Docker apunta a /certs y contiene ca.crt, node.crt y node.key.
TLS_DIR = os.environ.get("TLS_DIR", "")

# Secretos compartidos entre los MS y los DN. En producción se generan en scripts/start_cluster.sh.
TOKEN_SECRET = os.environ.get("TOKEN_SECRET", "dev-token-secret")
JWT_SECRET = os.environ.get("JWT_SECRET", "dev-jwt-secret")
JWT_TTL_S = _int("JWT_TTL", 900)
RAFT_PASSWORD = os.environ.get("RAFT_PASSWORD") or None

# Clave maestra del nodo de datos para AES-256. El valor por defecto es solo para pruebas.
MASTER_KEY_HEX = os.environ.get("MASTER_KEY_HEX", "00" * 32)

# Tiempos de mantenimiento: cuánto se espera antes de borrar físicamente, cada cuánto
# se reportan los bloques y cada cuánto se revisa la integridad en disco.
DELETE_GRACE_S = _int("GRACE_S", 300)
BLOCK_REPORT_INTERVAL_S = _int("REPORT_S", 3600)
SCRUB_INTERVAL_S = _int("SCRUB_S", 600)

# Semillas del cliente y archivo donde se guarda la sesión (JWT, cwd, id de cliente).
DEFAULT_MS = os.environ.get("DFSHA_MS", "127.0.0.1:7100")
SESSION_FILE = os.environ.get("DFSHA_SESSION", os.path.expanduser("~/.dfsha_session.json"))
