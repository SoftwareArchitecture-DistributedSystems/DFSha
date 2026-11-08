import glob, os, pytest
from tests.helpers import LOCAL, RUN, sha, wait_replicas


@pytest.mark.skipif(not LOCAL, reason="corrompe archivos del DN: solo en modo local")
def test_replica_corrupta_se_descarta_y_se_lee_otra(user):
    data = os.urandom(3_000_000)
    with user.open("c", "w") as h: h.write(data)
    locs = wait_replicas(user, "c", 3)
    bid = locs.blocks[0].block_id
    victim = locs.blocks[0].replicas[0]
    # escribimos directo en el archivo del DN para romper el tag GCM
    path = f"{RUN}/dn{int(victim.rsplit(':', 1)[1]) - 7200}/{bid}.blk"
    raw = bytearray(open(path, "rb").read()); raw[100] ^= 0xFF
    open(path, "wb").write(raw)
    # repetimos varias veces porque el orden de las réplicas es aleatorio
    for _ in range(5):
        with user.open("c") as r: assert sha(r.read()) == sha(data)
