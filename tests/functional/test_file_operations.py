import os, pytest
from tests.helpers import sha
from shared import constants as C
from shared.exceptions import DFSError


def test_open_write_close_read(user):
    # dos write seguidos tienen que verse como un solo archivo
    with user.open("f.txt", "w") as h:
        h.write(b"hola "); h.write(b"mundo")
    with user.open("f.txt", "r") as h:
        assert h.read(4) == b"hola" and h.read() == b" mundo"


def test_reemplazar_crea_nueva_version_y_anexar(user):
    # cada close que reemplaza el archivo sube la versión
    for text in (b"v1", b"version dos"):
        with user.open("f", "w") as h: h.write(text)
    assert user.ls()[0].version == 2
    with user.open("f", "a") as h: h.write(b"+extra")
    with user.open("f") as h: assert h.read() == b"version dos+extra"
    assert user.ls()[0].version == 3


def test_escritura_no_visible_hasta_close(user):
    # mientras el handle no se cierra, los lectores siguen viendo la versión anterior
    with user.open("g", "w") as h: h.write(b"original")
    h = user.open("g", "w"); h.write(b"nuevo")          # sin close
    with user.open("g") as r: assert r.read() == b"original"
    h.close()
    with user.open("g") as r: assert r.read() == b"nuevo"


def test_lector_termina_sobre_la_version_que_abrio(user, user2):
    # el lector no debería mezclar bloques de dos versiones distintas
    with user.open("h", "w") as w: w.write(b"A" * 100)
    r = user.open("h", "r")
    with user2.open("h", "w") as w: w.write(b"B" * 100)
    assert r.read() == b"A" * 100 and user.open("h").read() == b"B" * 100


def test_rm_y_archivo_inexistente(user):
    with user.open("z", "w") as h: h.write(b"x")
    user.rm("z")
    with pytest.raises(DFSError) as e: user.open("z", "r")
    assert e.value.code == "NOT_FOUND"


def test_lectura_por_rango_entre_bloques(user):
    # un byte más que BLOCK_SIZE para obligar a leer dos bloques
    data = os.urandom(C.BLOCK_SIZE + 1_000_000)
    with user.open("big", "w") as h: h.write(data)
    with user.open("big") as h:
        h.seek(C.BLOCK_SIZE - 10)
        assert h.read(30) == data[C.BLOCK_SIZE - 10:C.BLOCK_SIZE + 20]


def test_aislamiento_entre_usuarios(user, tmp_path):
    from tests.helpers import make_client
    otro = make_client(tmp_path)
    # cada usuario solo puede ver su propio /users/<uid>
    with pytest.raises(DFSError) as e: otro.ls(f"/users/{user.creds[0]}")
    assert e.value.code == "PERMISSION_DENIED"
    # el ".." no tiene que servir para escaparse
    with pytest.raises(DFSError) as e: otro.meta.list_dir("/users/otro/../" + user.creds[0])
    assert e.value.code == "PERMISSION_DENIED"
