#!/usr/bin/env bash
set -euo pipefail

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required"
  exit 1
fi

CLIENT_NAME="${1:-}"
if [[ -z "${CLIENT_NAME}" ]]; then
  echo "Usage: ./scripts/gen-client-cert.sh <name>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CERTS_DIR="${PROJECT_DIR}/certs"

CA_KEY="${CERTS_DIR}/ca.key"
CA_CRT="${CERTS_DIR}/ca.crt"
CA_SRL="${CERTS_DIR}/ca.srl"

if [[ ! -f "${CA_KEY}" || ! -f "${CA_CRT}" ]]; then
  echo "CA files not found. Generate them first:"
  echo "  ./scripts/gen-certs.sh <hostname>"
  exit 1
fi

CLIENT_KEY="${CERTS_DIR}/${CLIENT_NAME}.key"
CLIENT_CSR="${CERTS_DIR}/${CLIENT_NAME}.csr"
CLIENT_CRT="${CERTS_DIR}/${CLIENT_NAME}.crt"
CLIENT_P12="${CERTS_DIR}/${CLIENT_NAME}.p12"
CLIENT_EXT="${CERTS_DIR}/${CLIENT_NAME}.ext"

cat > "${CLIENT_EXT}" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF

openssl genrsa -out "${CLIENT_KEY}" 2048
openssl req -new -key "${CLIENT_KEY}" -out "${CLIENT_CSR}" -subj "/CN=${CLIENT_NAME}"
openssl x509 -req -in "${CLIENT_CSR}" -CA "${CA_CRT}" -CAkey "${CA_KEY}" \
  -CAserial "${CA_SRL}" -out "${CLIENT_CRT}" -days 825 -sha256 -extfile "${CLIENT_EXT}"
openssl pkcs12 -export -out "${CLIENT_P12}" -inkey "${CLIENT_KEY}" \
  -in "${CLIENT_CRT}" -certfile "${CA_CRT}" -passout pass:

echo "Generated client certificate:"
echo "  ${CLIENT_P12}"
