"""Pruebas de integración: requieren un clúster en marcha (scripts/start_cluster.sh [--local]).
Variables: DFSHA_MS (semillas MS), DFSHA_LOCAL=1 (clúster sin Docker), DFSHA_RUN (dir de pids/datos)."""
import hashlib, os, signal, socket, subprocess, sys, time, uuid
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


def _alive(pid):
    """True si el proceso existe y no es un zombi."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            # el formato es "pid (nombre con espacios) estado ..."
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except (FileNotFoundError, ProcessLookupError, IndexError):
        return False


def _pids(prefix, n):
    """Busca el proceso por su --data, por si el pidfile quedó viejo."""
    script = "metadata-server/main.py" if prefix == "ms" else "data-node/main.py"
    out = subprocess.run(["pgrep", "-f", rf"{script} .*--data {RUN}/{prefix}{n}( |$)"],
                         capture_output=True, text=True)
    return {int(x) for x in out.stdout.split()}


def _wait_port(port, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def node_ctl(action, addr):
    """Detiene o arranca un nodo (MS o DN). En Docker usa compose; en local relanza el proceso."""
    if not LOCAL:
        subprocess.run(["docker", "compose", action, addr.split(":")[0]], check=True, cwd=ROOT)
        return

    port = int(addr.rsplit(":", 1)[1])
    # Los MS usan 7100-7102 y los DN 7201-7205; de ahí sacamos el número de pid.
    is_ms = port < 7200
    prefix = "ms" if is_ms else "dn"
    n = port - 7100 if is_ms else port - 7200
    pidfile = os.path.join(RUN, f"{prefix}{n}.pid")

    if action == "stop":
        pids = _pids(prefix, n)
        try:
            with open(pidfile) as f:
                pids.add(int(f.read().strip()))
        except (FileNotFoundError, ProcessLookupError, ValueError):
            pass
        for pid in pids:
            if _alive(pid):
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        # Esperamos a que muera de verdad: si seguimos con el puerto ocupado,
        # el arranque siguiente falla y las pruebas se encadenan en SKIPPED.
        t0 = time.time()
        while any(_alive(p) for p in pids) and time.time() - t0 < 10:
            time.sleep(0.2)
        for pid in pids:
            if _alive(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        try:
            os.remove(pidfile)
        except FileNotFoundError:
            pass
        return

    # start: nos aseguramos de que no quede una instancia vieja en el puerto
    node_ctl("stop", addr)

    # reconstruimos el mismo comando que usa scripts/start_cluster.sh
    if is_ms:
        peers = ",".join(f"127.0.0.1:{7000 + j}" for j in range(3) if j != n)
        cmd = [sys.executable, str(ROOT / "metadata-server" / "main.py"),
               "--self", f"127.0.0.1:{7000 + n}", "--peers", peers,
               "--data", os.path.join(RUN, f"ms{n}")]
        log = os.path.join(RUN, f"ms{n}.log")
    else:
        cmd = [sys.executable, str(ROOT / "data-node" / "main.py"),
               "--addr", f"127.0.0.1:{port}", "--ms", ",".join(SEEDS),
               "--data", os.path.join(RUN, f"dn{n}")]
        log = os.path.join(RUN, f"dn{n}.log")

    os.makedirs(RUN, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join([str(ROOT), str(ROOT / "proto" / "generated")])
    with open(log, "ab") as out:
        proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT,
                                start_new_session=True, env=env)
    with open(pidfile, "w") as f:
        f.write(str(proc.pid))

    # No seguimos hasta que el nodo acepte conexiones. Así un fallo de arranque
    # se ve en este test y no como una cascada de SKIPPED en los siguientes.
    if not _wait_port(port) or not _alive(proc.pid):
        with open(log, errors="replace") as f:
            tail = "".join(f.readlines()[-20:])
        raise RuntimeError(f"no se pudo iniciar {prefix}{n} en {addr}:\n{tail}")


def wait_cluster(client, timeout=60):
    """Espera a que el clúster vuelva a tener líder después de una caída."""
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        try:
            client.meta.list_dir(client.abspath("."))
            return
        except DFSError as e:
            last = e
            time.sleep(1)
    raise AssertionError(f"el clúster no volvió a responder en {timeout}s: {last}")
