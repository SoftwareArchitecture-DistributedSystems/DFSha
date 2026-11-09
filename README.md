# DFSha

Sistema de archivos distribuido del curso Sistemas Distribuidos, Universidad Pontificia Bolivariana, 2026-2.

Arquitectura cliente/servidor con el servicio repartido entre servidores (S2S). El cliente parte cada archivo en bloques de 8 MiB. Cada bloque se copia en 3 nodos de datos y la escritura se confirma con 2 de esas 3 copias. Tres servidores de metadatos se ponen de acuerdo con Raft. La comunicación es gRPC, con mTLS en Docker. Los bloques se guardan cifrados con AES-256-GCM.

Nicolás Agudelo Mesa, María Alejandra Vargas Duque y David Alexander Salazar Villa.

## Qué se necesita

Para el arranque con contenedores:

- Git
- Docker Desktop abierto. Si el motor no está corriendo, `scripts/start_cluster.sh` igual imprime «clúster en marcha» y no crea los contenedores.

Para las pruebas automáticas, en la misma máquina y sin Docker:

- Python 3.11 o superior

## Arranque con Docker

Desde la raíz del repositorio:

```bash
git clone https://github.com/Alex-sklx0/DFSha.git
cd DFSha
bash scripts/start_cluster.sh
docker compose ps
```

La primera vez el script crea `certs/` (una CA privada y un certificado por nodo) y `.env` (secretos aleatorios). Esos archivos no se suben al repositorio. Luego construye las imágenes y levanta nueve contenedores: `ms1`, `ms2`, `ms3`, `dn1` a `dn5` y `client`.

`docker compose ps` tiene que mostrar los nueve en estado Up. Si la tabla sale vacía, abre Docker Desktop y vuelve a ejecutar el script.

Raft tarda unos segundos en elegir líder. Si el primer comando responde `NO_QUORUM` o `NOT_LEADER`, espera unos diez segundos y repítelo.

La contraseña debe tener al menos 8 caracteres. `DFSHA_PASS` evita que la pida por teclado.

```bash
docker compose exec client python client/main.py register ana
docker compose exec -e DFSHA_PASS=clave12345 client python client/main.py login ana
docker compose exec client python client/main.py pwd
docker compose exec client python client/main.py mkdir datos
docker compose exec client python client/main.py cd datos
docker compose exec client python client/main.py put /tmp/archivo.bin archivo.bin
docker compose exec client python client/main.py get archivo.bin /tmp/recuperado.bin
```

`pwd` muestra `/users/ana`. Cada usuario solo entra a su propia carpeta.

Para crear el archivo dentro del contenedor antes del `put`:

```bash
docker compose exec client python -c "import os; open('/tmp/archivo.bin','wb').write(os.urandom(20*1024*1024))"
```

Para copiar uno desde la máquina anfitriona:

```bash
docker compose cp archivo.bin client:/tmp/archivo.bin
```

Para apagar los contenedores, sin borrar los volúmenes:

```bash
bash scripts/stop_cluster.sh
```

Para borrar también los datos guardados:

```bash
bash scripts/clean_storage.sh
```

## Comandos del cliente

| Comando | Qué hace |
|---|---|
| `register <usuario>` | Crea la cuenta. Pide contraseña de 8 caracteres o más. |
| `login <usuario>` | Inicia sesión y deja el directorio en `/users/<usuario>`. |
| `pwd` | Muestra el directorio actual. Vive en el cliente. |
| `cd <ruta>` | Cambia el directorio actual. |
| `ls [ruta]` | Lista el directorio. |
| `mkdir <ruta>` | Crea un directorio. |
| `rmdir <ruta>` | Elimina un directorio vacío. |
| `rm <ruta>` | Elimina un archivo. |
| `put <local> <remoto>` | Sube un archivo. Crea o reemplaza. |
| `append <local> <remoto>` | Agrega el archivo local al final del remoto. |
| `get <remoto> <local>` | Descarga un archivo. |
| `read <ruta> <offset> <longitud>` | Lee un tramo y lo escribe en la salida estándar. |
| `write <ruta>` | Lee la entrada estándar y publica el archivo al cerrar. |
| `lock <ruta> <bloque\|-1> <X\|S>` | Pide un bloqueo. `-1` es el archivo completo. `X` es exclusivo y `S` es compartido. |
| `unlock <ruta> <bloque\|-1>` | Suelta el bloqueo. |

`write` no deja ver el contenido nuevo hasta que termina: por dentro hace `open`, `write` y `close`. Una escritura de menos de 8 MiB se queda en el cliente hasta ese cierre.

Dos procesos del mismo usuario son dos clientes si usan distinta sesión y distinto identificador:

```bash
docker compose exec -e DFSHA_SESSION=/tmp/a.json -e DFSHA_CLIENT_ID=clienteA -e DFSHA_PASS=clave12345 client python client/main.py login ana
docker compose exec -e DFSHA_SESSION=/tmp/a.json client python client/main.py lock datos/archivo.bin -1 X
```

El segundo, con `/tmp/b.json` y `clienteB`, recibe `BLOCK_LOCKED` mientras el primero no haga `unlock`.

## Arranque local y pruebas

Este modo no usa Docker ni mTLS. Sirve para correr pytest en la misma máquina. Los puertos quedan en `127.0.0.1`: metadatos en 7100, 7101 y 7102, y nodos de datos en 7201 a 7205.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
bash scripts/start_cluster.sh --local
export PYTHONPATH=$PWD:$PWD/proto/generated:$PWD/client
export DFSHA_MS=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
export DFSHA_LOCAL=1
python3 -m pytest tests -v
```

`tests/failures` tarda alrededor de dos minutos: espera a que un nodo se marque caído y se vuelva a copiar el bloque. Para medir un tamaño distinto:

```bash
DFSHA_PERF_MB=100 python3 -m pytest tests/performance -s
DFSHA_PERF_MB=1000 python3 -m pytest tests/performance -s
```

Para detener este modo:

```bash
bash scripts/stop_cluster.sh --local
bash scripts/clean_storage.sh --local
```

## Parámetros del sistema

| Parámetro | Valor |
|---|---|
| Tamaño de bloque | 8 MiB |
| Réplicas por bloque | 3 |
| Confirmaciones para escribir | 2 |
| Heartbeat | cada 3 s |
| Nodo sospechoso | 10 s sin heartbeat |
| Nodo caído | 30 s sin heartbeat |
| Lease de un bloqueo | 30 s, se renueva cada 10 s |
| JWT | 15 min |
| Puerto Raft entre metadatos | 7000 |
| Puerto gRPC de metadatos | 7100 |
| Puerto gRPC de un nodo de datos | 7001 |

## Limitaciones

- El tráfico Raft usa la contraseña de PySyncObj y la red privada. No va con mTLS.
- No hay KMS ni cifrado de extremo a extremo. Cada nodo de datos recibe su clave por `MASTER_KEY_HEX` y descifra el bloque para servirlo.
- El JWT es simétrico (HS256). Quien tenga `JWT_SECRET` puede firmar tokens.
- El registro de usuarios está abierto para quien llegue al servidor de metadatos.
- `write` crea, reemplaza o anexa. No modifica un rango dentro de un bloque ya guardado.
- Con Docker, los nueve procesos corren en la misma máquina. Parar un contenedor es la caída de ese nodo, no la caída de una máquina distinta.
