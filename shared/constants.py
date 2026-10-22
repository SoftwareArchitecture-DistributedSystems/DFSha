"""Constantes de diseño de DFSha. Son los valores que se justifican en el informe."""
# 8 MiB por bloque. Si se toca, hay que revisar las pruebas de lectura entre bloques.
BLOCK_SIZE = 8 * 1024 * 1024
# Los bloques se mandan por gRPC en pedazos de 1 MiB para no armar mensajes enormes.
CHUNK_SIZE = 1024 * 1024

# R = 3 copias por bloque y W = 2 ACK para dar la escritura por buena.
REPLICATION_FACTOR = 3
WRITE_QUORUM = 2

# Heartbeats cada 3 s. Con 3 perdidos el nodo pasa a sospechoso y a los 30 s se lo da por caído.
HEARTBEAT_INTERVAL_S = 3
SUSPECT_AFTER_S = 10
DEAD_AFTER_S = 30

# Los locks son leases: el cliente tiene que renovarlos antes de que venzan.
LOCK_TTL_S = 30
LOCK_RENEW_S = 10

# Una escritura abierta que queda abandonada se limpia a los 10 minutos.
PENDING_TTL_S = 600
WRITE_TOKEN_TTL_S = 120
READ_TOKEN_TTL_S = 300

# El puerto gRPC del MS se calcula como el puerto de Raft + 100 (7000 -> 7100).
MS_GRPC_OFFSET = 100
