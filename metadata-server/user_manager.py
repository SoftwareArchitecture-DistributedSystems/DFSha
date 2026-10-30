"""Usuarios, autenticación (argon2 + JWT) y autorización por espacio de nombres."""
import re

from shared.exceptions import DFSError, INVALID, PERMISSION_DENIED, UNAUTHENTICATED
from shared.security import create_jwt, hash_password, verify_jwt, verify_password
from shared.utils import normalize_path


class UserManager:
    def __init__(self, repo):
        self.repo = repo

    def register(self, uid, password):
        # el uid va en minúsculas y sin espacios para que sea usable como nombre de directorio
        if not re.fullmatch(r"[a-z0-9_]{3,32}", uid) or len(password) < 8:
            raise DFSError(INVALID, "uid [a-z0-9_]{3,32} y contraseña de al menos 8 caracteres")
        # el hash se calcula acá, fuera de Raft, porque argon2 no es determinista (usa sal)
        self.repo.raft("register", uid=uid, pw=hash_password(password))

    def authenticate(self, uid, password):
        if not verify_password(self.repo.users.get(uid, ""), password):
            # mismo mensaje para usuario inexistente y contraseña mala, para no filtrar datos
            raise DFSError(UNAUTHENTICATED, "usuario o contraseña incorrectos")
        return create_jwt(uid)

    @staticmethod
    def uid_from_token(token):
        return verify_jwt(token)

    @staticmethod
    def authorize_path(path, uid):
        """ACL simple: cada usuario solo puede moverse dentro de /users/<uid>/."""
        p = normalize_path(path)
        home = f"/users/{uid}"
        # el startswith con "/" evita que /users/david2 pase como si fuera /users/david
        if not p.startswith("/") or (p != home and not p.startswith(home + "/")):
            raise DFSError(PERMISSION_DENIED, "ruta fuera de tu espacio")
        return p
