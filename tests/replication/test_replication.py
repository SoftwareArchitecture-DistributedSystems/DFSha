import os
from tests.helpers import wait_replicas


def test_tres_replicas_en_nodos_distintos(user):
    # 20 MB son 3 bloques; cada uno tiene que estar en 3 nodos diferentes
    with user.open("r", "w") as h: h.write(os.urandom(20_000_000))
    locs = wait_replicas(user, "r", 3)
    assert len(locs.blocks) == 3
    for b in locs.blocks:
        assert len(b.replicas) == 3 and len(set(b.replicas)) == 3


def test_bloques_repartidos_entre_varios_nodos(user):
    # con 5 bloques no deberían caer todos en los mismos tres nodos
    with user.open("d", "w") as h: h.write(os.urandom(40_000_000))
    nodos = {r for b in wait_replicas(user, "d", 3).blocks for r in b.replicas}
    assert len(nodos) >= 4, (
        f"solo {len(nodos)} DN recibieron bloques: {sorted(nodos)}. "
        "Revisa que los cinco nodos estén arriba antes de correr la suite: "
        "scripts/start_cluster.sh --local (es idempotente y reinicia el clúster).")
