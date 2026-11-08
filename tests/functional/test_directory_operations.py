import pytest
from shared.exceptions import DFSError


def test_mkdir_ls_rmdir(user):
    user.mkdir("a"); user.mkdir("a/b")
    assert [e.name for e in user.ls("a")] == ["b"]
    # rmdir no borra si hay algo adentro, igual que en Unix
    with pytest.raises(DFSError) as e: user.rmdir("a")
    assert e.value.code == "NOT_EMPTY"
    user.rmdir("a/b"); user.rmdir("a")
    assert user.ls() == []


def test_duplicados_y_padre_inexistente(user):
    user.mkdir("d")
    with pytest.raises(DFSError) as e: user.mkdir("d")
    assert e.value.code == "ALREADY_EXISTS"
    # no creamos directorios intermedios automáticamente
    with pytest.raises(DFSError) as e: user.mkdir("x/y/z")
    assert e.value.code == "NOT_FOUND"


def test_cd_es_estado_local(user):
    # cd no habla con el servidor: solo cambia el cwd de la sesión
    user.mkdir("c"); user.cd("c")
    assert user.pwd().endswith("/c")
    user.mkdir("sub")
    assert [e.name for e in user.ls()] == ["sub"]
