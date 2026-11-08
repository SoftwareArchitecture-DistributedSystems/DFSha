"""Pruebas de integración: requieren un clúster en marcha (scripts/start_cluster.sh [--local]).
Variables: DFSHA_MS (semillas MS), DFSHA_LOCAL=1 (clúster sin Docker), DFSHA_RUN (dir de pids/datos)."""
import hashlib, os, subprocess, sys, time, uuid
from pathlib import Path

import pytest

# pytest no corre desde la raíz del repo, así que agregamos a mano los paths
# donde viven el cliente y los stubs generados.
ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "proto" / "generated", ROOT / "client"):
    sys.path.insert(0, str(p))

from dfs_client import DFSClient            # noqa: E402
from shared.exceptions import DFSError      # noqa: E402

SEEDS = os.environ.get("DFSHA_MS", "127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102").split(",")
LOCAL = bool(os.environ.get("DFSHA_LOCAL"))
RUN = os.environ.get("DFSHA_RUN", "/tmp/dfsha_run")


def sha(b): return hashlib.sha256(b).hexdigest()


def make_client(tmp_path, uid=None, cid=None, password="password123"):
    # el uid aleatorio evita choques si se corre la suite varias veces sin limpiar
    uid = uid or "t" + uuid.uuid4().hex[:10]
    c = DFSClient(SEEDS, str(tmp_path / f"{uuid.uuid4().hex[:6]}.json"), cid)
    try:
        # registramos una sola vez por proceso; el atributo known vive en la función
        if uid not in getattr(make_client, "known", set()):
            c.register(uid, password)
            make_client.known = getattr(make_client, "known", set()) | {uid}
        c.login(uid, password)
    except DFSError as e:
        if e.code == "UNAVAILABLE":
            # sin cluster no tiene sentido fallar; se salta el test
            pytest.skip("no hay clúster DFSha en marcha")
        raise
    c.creds = (uid, password)
    return c


def wait_replicas(client, path, n=3, timeout=40):
    # La re-replicación es asíncrona; damos un tiempo antes de dar el test por fallado.
    t0 = time.time()
    while time.time() - t0 < timeout:
        locs = client.meta.get_block_locations(client.abspath(path))
        if all(len(set(b.replicas)) >= n for b in locs.blocks):
            return locs
        time.sleep(2)
    return client.meta.get_block_locations(client.abspath(path))


def node_ctl(action, addr):
    """Detiene o arranca un DN: docker compose, o kill del proceso en modo local."""
    if LOCAL:
        port = int(addr.rsplit(":", 1)[1])
        # Los MS usan 7100-7102 y los DN 7201-7205; de ahí sacamos el número de pid.
        n = port - 7100 if port < 7200 else port - 7200
        if port < 7200:
            os.system(f"kill $(cat {RUN}/ms{n}.pid) 2>/dev/null")
            return
        if action == "stop":
            os.system(f"kill $(cat {RUN}/dn{n}.pid) 2>/dev/null")
        else:
            # En local no tenemos un supervisor que lo vuelva a levantar.
            pytest.skip("reinicio de DN solo disponible con Docker")
    else:
        subprocess.run(["docker", "compose", action, addr.split(":")[0]], check=True, cwd=ROOT)
