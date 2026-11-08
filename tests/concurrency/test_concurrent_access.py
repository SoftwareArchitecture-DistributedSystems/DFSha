import os, threading, pytest
from tests.helpers import make_client, sha
from shared.exceptions import DFSError


def test_lock_exclusivo_entre_clientes(user, user2):
    with user.open("l", "w") as h: h.write(b"x")
    user.lock("l", 0, "X")
    # el segundo cliente no puede tomar el mismo lock
    with pytest.raises(DFSError) as e: user2.lock("l", 0, "X")
    assert e.value.code == "BLOCK_LOCKED"
    # anexar también exige el lock exclusivo del archivo
    with pytest.raises(DFSError) as e: user2.open("l", "a")
    assert e.value.code == "BLOCK_LOCKED"
    user.unlock("l", 0)
    user2.lock("l", 0, "X")


def test_locks_compartidos_conviven(user, user2):
    # dos lectores (S) pueden tener el lock al mismo tiempo...
    with user.open("s", "w") as h: h.write(b"x")
    user.lock("s", -1, "S"); user2.lock("s", -1, "S")
    # ...pero uno exclusivo no
    with pytest.raises(DFSError) as e: user.lock("s", -1, "X")
    assert e.value.code == "BLOCK_LOCKED"


def test_escritura_de_otro_cliente_sobre_bloque_bloqueado(user, user2):
    with user.open("w", "w") as h: h.write(b"x")
    user.lock("w", 0, "X")
    with pytest.raises(DFSError) as e:
        with user2.open("w", "w") as h: h.write(b"y")
    assert e.value.code == "BLOCK_LOCKED"


def test_version_conflict_gana_un_solo_escritor(user, user2):
    with user.open("v", "w") as h: h.write(b"base")
    # los dos abren sobre la misma versión; el que cierra primero gana
    a, b = user.open("v", "w"), user2.open("v", "w")
    a.write(b"ganador"); a.close()
    b.write(b"perdedor")
    with pytest.raises(DFSError) as e: b.close()
    assert e.value.code == "VERSION_CONFLICT"
    with user.open("v") as r: assert r.read() == b"ganador"


def test_subidas_paralelas(user, tmp_path):
    datos, errores = {}, []

    def work(i):
        try:
            # cada hilo usa su propio cliente y su propio cid
            c = make_client(tmp_path, uid=user.creds[0], cid=f"c{i}")
            d = os.urandom(3_000_000 + i); datos[i] = d
            with c.open(f"p{i}", "w") as h: h.write(d)
        except Exception as e: errores.append(e)

    ts = [threading.Thread(target=work, args=(i,)) for i in range(4)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not errores
    for i, d in datos.items():
        with user.open(f"p{i}") as r: assert sha(r.read()) == sha(d)
