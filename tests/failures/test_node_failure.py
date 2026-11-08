import os, time, pytest
from tests.helpers import node_ctl, sha, wait_replicas


def test_lectura_y_escritura_con_un_dn_caido(user):
    data = os.urandom(10_000_000)
    with user.open("f", "w") as h: h.write(data)
    victima = wait_replicas(user, "f", 3).blocks[0].replicas[0]
    node_ctl("stop", victima)
    try:
        # la lectura tiene que caer en otra réplica sin que el cliente note nada
        with user.open("f") as r: assert sha(r.read()) == sha(data)
        nuevo = os.urandom(10_000_000)
        # la escritura sigue funcionando porque W=2 y quedan dos nodos vivos
        with user.open("g", "w") as h: h.write(nuevo)
        with user.open("g") as r: assert sha(r.read()) == sha(nuevo)
    finally:
        # en modo local no hay supervisor, así que no intentamos levantarlo
        node_ctl("start", victima) if not os.environ.get("DFSHA_LOCAL") else None


def test_re_replicacion_tras_caida(user):
    with user.open("rr", "w") as h: h.write(os.urandom(5_000_000))
    victima = wait_replicas(user, "rr", 3).blocks[0].replicas[0]
    node_ctl("stop", victima)
    t0 = time.time()
    # ~30 s para declararlo caído y después el tiempo de copiar el bloque
    while time.time() - t0 < 120:
        b = user.meta.get_block_locations(user.abspath("rr")).blocks[0]
        if len(b.replicas) >= 3 and victima not in b.replicas:
            return
        time.sleep(3)
    pytest.fail("no se restauró el factor de replicación 3")
