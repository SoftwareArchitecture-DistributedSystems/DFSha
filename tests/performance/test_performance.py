import json, os, time
from tests.helpers import sha

MB = int(os.environ.get("DFSHA_PERF_MB", "50"))


def test_throughput(user, tmp_path, capsys):
    # DFSHA_PERF_MB permite subir el tamaño sin tocar el código
    src, dst = tmp_path / "p", tmp_path / "q"
    src.write_bytes(os.urandom(MB * 1_000_000))
    t0 = time.time(); user.put(str(src), "perf"); put_s = time.time() - t0
    t0 = time.time(); user.get("perf", str(dst)); get_s = time.time() - t0
    t0 = time.time(); user.ls(); user.meta.get_block_locations(user.abspath("perf")); meta_ms = (time.time() - t0) * 1000
    assert sha(src.read_bytes()) == sha(dst.read_bytes())
    r = {"MB": MB, "put_MBps": round(MB / put_s, 1), "get_MBps": round(MB / get_s, 1), "metadatos_ms": round(meta_ms, 1)}
    # capsys.disabled para que el resultado se vea aunque pytest capture la salida
    with capsys.disabled(): print("\nRESULTADO", json.dumps(r))
    assert r["put_MBps"] > 0 and r["get_MBps"] > 0
