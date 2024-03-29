#! /bin/bash
set -e

HERE=${0%/*}
DEST=${DEST:-${HERE}/.sync/certs}
RENEW_CA=${RENEW_CA:-}
OPENSSL=${OPENSSL:-openssl}

KUBECTL=${KUBECTL:-kubectl}
export KUBECONFIG=${KUBECONFIG:-}

function message() {
	echo "$@" >&2
}


function get_node_ip() {
	${KUBECTL} get nodes -o jsonpath='{.items[*].status.addresses[?(@.type=="InternalIP")].address}'
}

function alt_names() {
    for NAME in $(get_node_ip) ; do
        COUNT=$((COUNT + 1))
        echo "IP.${COUNT}=${NAME}"
    done
}

# ------
mkdir -p ${DEST}

# ... certificate
if [ -n "${RENEW_CA}" ] ; then
   message "generating self-signed CA certificate & key..."
    # create self-signed CA certificate/key pair
    # ... key

    ${OPENSSL} genrsa -out ${DEST}/ca.key 4096

    ${OPENSSL} req -new -x509 -nodes \
        -key ${DEST}/ca.key \
        -sha256 -days 3650 \
        -out ${DEST}/ca.crt \
        -subj "/CN=knitfab/O=knitfab/OU=knitfab"
fi


message "generating server certificate & key..."
# create server key
${OPENSSL} genrsa -out ${DEST}/server.key 4096


cat <<EOF > ${DEST}/san.extfile
[req]
distinguished_name = req_distinguished_name
req_extensions = req_ext
prompt = no

[req_distinguished_name]
CN = knitfab

[ req_ext ]
subjectAltName=@alt_names

[ SAN ]
subjectAltName=@alt_names
basicConstraints=CA:FALSE

[ v3_ext ]
authorityKeyIdentifier=keyid,issuer:always
basicConstraints=CA:FALSE
keyUsage=keyEncipherment,dataEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=@alt_names

[alt_names]
$(alt_names)
EOF

# create server CSR
${OPENSSL} req -new \
    -key ${DEST}/server.key -out ${DEST}/server.csr -config ${DEST}/san.extfile

# create server certificate
${OPENSSL} x509 -req -in ${DEST}/server.csr \
    -CA ${DEST}/ca.crt -CAkey ${DEST}/ca.key -CAcreateserial \
    -out ${DEST}/server.crt \
    -extensions v3_ext -extfile ${DEST}/san.extfile \
    -days 3650 -sha256

message "cetificates generated."
message ""
