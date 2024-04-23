#! /bin/bash
set -e

KUBECTL=${KUBECTL:-kubectl}

SECRET=${SECRET:-database-credential}

NAMESPACE=${NAMESPACE:-knitfab}
DEST=${1}

if [ -z "${NAMESPACE}" ]; then
  echo "NAMESPACE is not specified!" >&2
  exit 1
fi

if [ -z "${DEST}" ] ; then
  echo "Usage: ${0} <DEST_DIR>" >&2
  exit 1
fi

mkdir -p "${DEST}"

echo "* Backing up database from ${NAMESPACE} to ${DEST}" >&2
echo "* Creating a pod named \"pgdumper\"" >&2
cat <<EOF | ${KUBECTL} -n ${NAMESPACE} apply --wait -f - | sed -Ee "s/^/ > ${KUBECTL} | /g"
apiVersion: v1
kind: Pod
metadata:
  name: pgdumper
  namespace: ${NAMESPACE}
spec:
  containers:
  - name: pgdumper
    image: postgres:15.6-bullseye
    command: ["sleep", "infinity"]
    env:
    - name: PGHOST
      value: ${PGHOST}
    - name: PGPORT
      value: "${PGPORT:-5432}"
    - name: PGPASSWORD
      valueFrom:
        secretKeyRef:
          name: ${SECRET}
          key: password
    - name: PGUSER
      valueFrom:
        secretKeyRef:
          name: ${SECRET}
          key: username
EOF
${KUBECTL} -n ${NAMESPACE} wait --timeout=-1s --for=condition=Ready "pod/pgdumper" | sed -Ee "s/^/ > ${KUBECTL} | /g"

echo "* Dumping database..." >&2
${KUBECTL} exec pgdumper -n ${NAMESPACE} -- sh -c 'pg_dump -F tar' | gzip > "${DEST}/dump.tar.gz"

echo "* Cleaning up the pod..." >&2
${KUBECTL} delete pod pgdumper -n ${NAMESPACE} --now | sed -Ee "s/^/ > ${KUBECTL} | /g"
${KUBECTL} -n ${NAMESPACE} wait --for="delete" --timeout=-1s pod pgdumper | sed -e "s/^/ > ${KUBECTL} | /g"

echo "* done" >&2
