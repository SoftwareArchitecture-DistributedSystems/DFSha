import os, time
from tests.helpers import node_ctl, sha


def test_caida_del_lider_de_metadatos(user):
    data = os.urandom(4_000_000)
    with user.open("antes", "w") as h: h.write(data)
    # el router ya sabe quién es el líder después de las escrituras anteriores
    leader = user.meta.router.leader
    t0 = time.time(); node_ctl("stop", leader)
    # el cliente debería esperar la elección y reintentar solo
    with user.open("despues", "w") as h: h.write(b"sigo escribiendo")
    print(f"\nrecuperación tras caída del líder: {time.time() - t0:.1f}s")
    # lo que ya estaba confirmado no se pierde
    with user.open("antes") as r: assert sha(r.read()) == sha(data)
    with user.open("despues") as r: assert r.read() == b"sigo escribiendo"
