import os, time
from tests.helpers import node_ctl, sha, wait_cluster


def test_caida_del_lider_de_metadatos(user):
    data = os.urandom(4_000_000)
    with user.open("antes", "w") as h: h.write(data)
    # el router ya sabe quién es el líder después de las escrituras anteriores
    leader = user.meta.router.leader
    assert leader, "el cliente todavía no detectó un líder de metadatos"
    t0 = time.time(); node_ctl("stop", leader)
    try:
        # el cliente debería esperar la elección y reintentar solo
        with user.open("despues", "w") as h: h.write(b"sigo escribiendo")
        print(f"\nrecuperación tras caída del líder: {time.time() - t0:.1f}s")
        # lo que ya estaba confirmado no se pierde
        with user.open("antes") as r: assert sha(r.read()) == sha(data)
        with user.open("despues") as r: assert r.read() == b"sigo escribiendo"
    finally:
        # devolvemos el MS a la vida y esperamos a que el clúster recupere quórum
        node_ctl("start", leader)
        wait_cluster(user)
