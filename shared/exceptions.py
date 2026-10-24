"""Códigos de error de la sección 6.6 y la excepción que usan todos los módulos."""
# Los códigos viajan como texto dentro de Status para que el cliente no dependa
# de los códigos numéricos de gRPC.
UNAUTHENTICATED = "UNAUTHENTICATED"
PERMISSION_DENIED = "PERMISSION_DENIED"
NOT_FOUND = "NOT_FOUND"
ALREADY_EXISTS = "ALREADY_EXISTS"
NOT_LEADER = "NOT_LEADER"
NO_QUORUM = "NO_QUORUM"
INSUFFICIENT_REPLICAS = "INSUFFICIENT_REPLICAS"
VERSION_CONFLICT = "VERSION_CONFLICT"
BLOCK_LOCKED = "BLOCK_LOCKED"
DATA_LOSS = "DATA_LOSS"
UNAVAILABLE = "UNAVAILABLE"
INVALID = "INVALID"
NOT_EMPTY = "NOT_EMPTY"
INCOMPLETE = "INCOMPLETE"


class DFSError(Exception):
    """Error de negocio. `leader` se usa cuando el MS que respondió no es el líder."""

    def __init__(self, code, message="", leader=""):
        super().__init__(f"{code}: {message}")
        self.code, self.message, self.leader = code, message, leader
