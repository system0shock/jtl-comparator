#!/usr/bin/env bash
set -euo pipefail

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required"
  exit 1
fi

HOSTNAME="${1:-}"
if [[ -z "${HOSTNAME}" ]]; then
  echo "Usage: ./scripts/gen-certs.sh <hostname>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CERTS_DIR="${PROJECT_DIR}/certs"

mkdir -p "${CERTS_DIR}"

CA_KEY="${CERTS_DIR}/ca.key"
CA_CRT="${CERTS_DIR}/ca.crt"
CA_SRL="${CERTS_DIR}/ca.srl"
SERVER_KEY="${CERTS_DIR}/server.key"
SERVER_CSR="${CERTS_DIR}/server.csr"
SERVER_CRT="${CERTS_DIR}/server.crt"
SERVER_EXT="${CERTS_DIR}/server.ext"
CLIENT_KEY="${CERTS_DIR}/client.key"
CLIENT_CSR="${CERTS_DIR}/client.csr"
CLIENT_CRT="${CERTS_DIR}/client.crt"
CLIENT_P12="${CERTS_DIR}/client.p12"
CLIENT_EXT="${CERTS_DIR}/client.ext"

rm -f "${CA_SRL}" "${SERVER_EXT}" "${CLIENT_EXT}"

openssl genrsa -out "${CA_KEY}" 4096
openssl req -x509 -new -nodes -key "${CA_KEY}" -sha256 -days 3650 \
  -out "${CA_CRT}" -subj "/CN=JTL Comparator Test CA"

openssl genrsa -out "${SERVER_KEY}" 2048
openssl req -new -key "${SERVER_KEY}" -out "${SERVER_CSR}" -subj "/CN=${HOSTNAME}"
cat > "${SERVER_EXT}" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:${HOSTNAME},DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in "${SERVER_CSR}" -CA "${CA_CRT}" -CAkey "${CA_KEY}" \
  -CAcreateserial -CAserial "${CA_SRL}" -out "${SERVER_CRT}" -days 825 -sha256 -extfile "${SERVER_EXT}"

openssl genrsa -out "${CLIENT_KEY}" 2048
openssl req -new -key "${CLIENT_KEY}" -out "${CLIENT_CSR}" -subj "/CN=sample.engineer"
cat > "${CLIENT_EXT}" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -in "${CLIENT_CSR}" -CA "${CA_CRT}" -CAkey "${CA_KEY}" \
  -CAserial "${CA_SRL}" -out "${CLIENT_CRT}" -days 825 -sha256 -extfile "${CLIENT_EXT}"

openssl pkcs12 -export -out "${CLIENT_P12}" -inkey "${CLIENT_KEY}" \
  -in "${CLIENT_CRT}" -certfile "${CA_CRT}" -passout pass:

echo "Generated test certificates in ${CERTS_DIR}:"
echo "  ${CA_CRT}"
echo "  ${SERVER_CRT}"
echo "  ${SERVER_KEY}"
echo "  ${CLIENT_CRT}"
echo "  ${CLIENT_KEY}"
echo "  ${CLIENT_P12}"
echo
echo "Usage example:"
echo "  TLS_CERT=${SERVER_CRT} TLS_KEY=${SERVER_KEY} TLS_CA=${CA_CRT} python app.py"
