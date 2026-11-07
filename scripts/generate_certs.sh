#!/bin/bash
# Genera una CA privada y un certificado por nodo. El mismo certificado sirve
# como servidor y como cliente, que es justo lo que necesita mTLS.
# Uso: scripts/generate_certs.sh <dir> nombre1 nombre2 ...   (el nombre es el host de Docker/DNS)
set -e
OUT=$1; shift; mkdir -p "$OUT"; cd "$OUT"

# La CA se reutiliza si ya existe, así no cambian todos los certs en cada arranque.
[ -f ca.key ] || openssl req -x509 -newkey rsa:4096 -nodes -keyout ca.key -out ca.crt -days 365 -subj "/CN=DFSha-CA" 2>/dev/null

for N in "$@"; do
  mkdir -p $N
  openssl req -newkey rsa:2048 -nodes -keyout $N/node.key -out $N/node.csr -subj "/CN=$N" 2>/dev/null
  # El SAN tiene que incluir el nombre del servicio y localhost para las pruebas locales.
  printf "subjectAltName=DNS:$N,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n" > $N/ext.cnf
  openssl x509 -req -in $N/node.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out $N/node.crt -days 365 -extfile $N/ext.cnf 2>/dev/null
  cp ca.crt $N/ca.crt; rm $N/node.csr $N/ext.cnf
done
echo "certificados en $OUT/<nombre>/{node.crt,node.key,ca.crt}"
