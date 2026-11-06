"""CLI de DFSha. Es un despachador simple: cada comando llama a un método de DFSClient."""
import argparse, getpass, os, sys

from dfs_client import DFSClient
from shared.exceptions import DFSError

# El "?" marca argumentos opcionales. Lo usamos para no repetir argparse en cada comando.
COMMANDS = {"register": ["uid"], "login": ["uid"], "ls": ["?path"], "cd": ["path"], "pwd": [],
            "mkdir": ["path"], "rmdir": ["path"], "rm": ["path"], "put": ["local", "remote"],
            "append": ["local", "remote"], "get": ["remote", "local"], "read": ["remote", "offset", "length"],
            "write": ["remote"], "lock": ["path", "block", "mode"], "unlock": ["path", "block"]}


def run(a, c):
    cmd = a.cmd
    if cmd in ("register", "login"):
        # DFSHA_PASS está pensado para las pruebas; en uso normal se pide por consola
        pw = os.environ.get("DFSHA_PASS") or getpass.getpass("contraseña: ")
        (c.register if cmd == "register" else c.login)(a.uid, pw)
        print("ok")
    elif cmd == "pwd":
        print(c.pwd())
    elif cmd == "cd":
        c.cd(a.path)
    elif cmd == "ls":
        # formato parecido a ls -l, pero sin permisos ni fechas
        for e in c.ls(a.path):
            print(f"{'d' if e.is_dir else '-'} {e.size:>12} v{e.version} {e.name}")
    elif cmd == "mkdir":
        c.mkdir(a.path); print("ok")
    elif cmd == "rmdir":
        c.rmdir(a.path); print("ok")
    elif cmd == "rm":
        c.rm(a.path); print("ok")
    elif cmd in ("put", "append"):
        # put reemplaza y append agrega al final (con lock exclusivo)
        size, dt = c.put(a.local, a.remote, "w" if cmd == "put" else "a")
        print(f"{size/1e6:.1f} MB en {dt:.2f}s ({size/1e6/max(dt, 1e-6):.1f} MB/s)")
    elif cmd == "get":
        n, dt = c.get(a.remote, a.local)
        print(f"{n/1e6:.1f} MB en {dt:.2f}s ({n/1e6/max(dt, 1e-6):.1f} MB/s)")
    elif cmd == "read":
        # escribe bytes crudos a stdout, así se puede redirigir a un archivo
        with c.open(a.remote, "r") as h:
            h.seek(int(a.offset))
            sys.stdout.buffer.write(h.read(int(a.length)))
    elif cmd == "write":
        # open("w") -> write(stdin) -> close(): recién al cerrar se publica la versión
        with c.open(a.remote, "w") as h:
            h.write(sys.stdin.buffer.read())
        print("ok")
    elif cmd == "lock":
        print(f"lock hasta {c.lock(a.path, int(a.block), a.mode)} (repite el comando para renovar)")
    elif cmd == "unlock":
        c.unlock(a.path, int(a.block)); print("ok")


def main():
    ap = argparse.ArgumentParser(prog="dfsha")
    sub = ap.add_subparsers(dest="cmd", required=True)
    # armamos los subcomandos a partir de COMMANDS para no escribir uno por uno
    for name, args in COMMANDS.items():
        p = sub.add_parser(name)
        for arg in args:
            p.add_argument(arg.strip("?"), nargs="?" if arg.startswith("?") else None)
    a = ap.parse_args()
    c = DFSClient()
    # register y login son los únicos que no necesitan sesión previa
    if a.cmd not in ("register", "login") and not c.session.data["jwt"]:
        sys.exit("error: haz login primero")
    try:
        run(a, c)
    except DFSError as e:
        sys.exit(f"error: {e.code}: {e.message}")
