import os
from tests.helpers import sha


def test_put_get_20mb(user, tmp_path):
    # 20 MB alcanzan para usar varios bloques de 8 MiB
    src, dst = tmp_path / "in.bin", tmp_path / "out.bin"
    src.write_bytes(os.urandom(20_000_000))
    user.put(str(src), "video.bin"); user.get("video.bin", str(dst))
    assert sha(dst.read_bytes()) == sha(src.read_bytes())
    assert user.ls()[0].size == 20_000_000


def test_archivo_vacio(user, tmp_path):
    # el caso borde de 0 bytes suele romper las implementaciones de bloques
    src, dst = tmp_path / "e", tmp_path / "e2"
    src.write_bytes(b"")
    user.put(str(src), "vacio"); user.get("vacio", str(dst))
    assert dst.read_bytes() == b""


def test_put_sobre_existente_reemplaza(user, tmp_path):
    for n in (1000, 5000):
        f = tmp_path / "x"; f.write_bytes(os.urandom(n)); user.put(str(f), "x")
    assert user.ls()[0].size == 5000 and user.ls()[0].version == 2
