"""Seguridad: contraseñas (argon2), JWT, capability tokens, claves por bloque (HKDF) y TLS/mTLS."""
import base64, hashlib, hmac, json, time

import grpc, jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from shared import config
from shared.exceptions import DFSError, UNAUTHENTICATED

# argon2 con los parámetros que trae por defecto. En un despliegue real habría que fijarlos
# según el hardware, pero para el trabajo nos sirve así.
_ph = PasswordHasher()

# gRPC corta los mensajes grandes por defecto. Los bloques van de 1 MiB en 1 MiB, pero
# dejamos margen por si algún día se sube el CHUNK_SIZE.
GRPC_OPTS = [("grpc.max_receive_message_length", 16 * 1024 * 1024),
             ("grpc.max_send_message_length", 16 * 1024 * 1024)]


def hash_password(password: str) -> str:
    # argon2 ya guarda la sal y los parámetros dentro del propio hash
    return _ph.hash(password)


def verify_password(hashed: str, password: str) -> bool:
    # si el usuario no existe llega un string vacío; preferimos false antes que una excepción
    try:
        return bool(hashed) and _ph.verify(hashed, password)
    except (VerifyMismatchError, InvalidHashError):
        return False


def create_jwt(uid: str):
    # exp va en segundos desde epoch, que es lo que espera PyJWT
    exp = int(time.time()) + config.JWT_TTL_S
    return jwt.encode({"sub": uid, "exp": exp}, config.JWT_SECRET, "HS256"), exp


def verify_jwt(token: str) -> str:
    try:
        return jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])["sub"]
    except jwt.PyJWTError:
        # PyJWT tiene varias excepciones para esto; para el cliente todas significan lo mismo
        raise DFSError(UNAUTHENTICATED, "JWT ausente, inválido o expirado")


# Los capability tokens los emite el MS y los valida el DN. Así el DN no tiene que
# consultar al MS en cada lectura o escritura de bloque.
def make_token(block_id: str, op: str, uid: str = "", ttl: int = 120) -> str:
    body = base64.urlsafe_b64encode(json.dumps(
        {"b": block_id, "o": op, "u": uid, "e": int(time.time()) + ttl}).encode()).decode()
    sig = hmac.new(config.TOKEN_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    return f"{body}.{sig}"


def check_token(token: str, block_id: str, op: str) -> bool:
    """op: 'w' escribir, 'r' leer, 'x' orden administrativa de MS (replicar/borrar/consultar)."""
    try:
        body, sig = token.split(".")
        good = hmac.new(config.TOKEN_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
        # compare_digest evita que el tiempo de comparación delate la firma
        if not hmac.compare_digest(sig, good):
            return False
        d = json.loads(base64.urlsafe_b64decode(body))
        return d["b"] == block_id and d["o"] == op and d["e"] > time.time()
    except Exception:
        # token mal formado, base64 roto, JSON inválido... todo se trata como inválido
        return False


# Clave por bloque y versión. Si el bloque se reemplaza, la versión cambia y también la clave.
def derive_block_key(master: bytes, block_id: str, version: int) -> bytes:
    return HKDF(hashes.SHA256(), 32, None, f"{block_id}|{version}".encode()).derive(master)


# TLS/mTLS sobre gRPC. El mismo par de certificados sirve para servidor y cliente,
# por eso el DN puede abrir conexiones a otro DN sin tener un cert aparte.
def _read(path):
    with open(path, "rb") as f:
        return f.read()


def server_credentials():
    d = config.TLS_DIR
    if not d:
        # sin TLS_DIR el servidor arranca en claro; sirve para desarrollo, no para AWS
        return None
    return grpc.ssl_server_credentials([(_read(f"{d}/node.key"), _read(f"{d}/node.crt"))],
                                       root_certificates=_read(f"{d}/ca.crt"), require_client_auth=True)


def open_channel(addr: str):
    d = config.TLS_DIR
    if not d:
        return grpc.insecure_channel(addr, options=GRPC_OPTS)
    creds = grpc.ssl_channel_credentials(root_certificates=_read(f"{d}/ca.crt"),
                                         private_key=_read(f"{d}/node.key"),
                                         certificate_chain=_read(f"{d}/node.crt"))
    return grpc.secure_channel(addr, creds, options=GRPC_OPTS)
