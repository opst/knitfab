#! /bin/bash
set -e

KUBECTL=${KUBECTL:-kubectl}
if [ -z "${KUBECTL}" ] ; then
  echo "KUBECTL is not specified!" >&2
  exit 1
fi

JQ=${JQ:-jq}
if [ -z "${JQ}" ] ; then
  echo "JQ is not specified!" >&2
  exit 1
fi

NAMESPACE=${NAMESPACE:-knitfab}
if [ -z "${NAMESPACE}" ] ; then
  echo "NAMESPACE is not specified!" >&2
  exit 1
fi

SECRET=${SECRET:-database-credential}
if [ -z "${SECRET}" ] ; then
  echo "SECRET is not specified!" >&2
  exit 1
fi

SOURCE=${1}
if [ -z "${SOURCE}" ] ; then
  echo "Usage: $0 <SOURCE_DIR>" >&2
  exit 1
fi

echo "* Restore database from ${NAMESPACE} to ${DEST}" >&2
echo "* Creating a pod named \"pgloader\"" >&2
cat <<EOF | ${KUBECTL} -n ${NAMESPACE} apply --wait -f - | sed -Ee "s/^/ > ${KUBECTL} | /g"
apiVersion: v1
kind: Pod
metadata:
  name: pgloader
  namespace: ${NAMESPACE}
spec:
  containers:
  - name: pgpgloaderdumper
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
    - name: PGDATABASE
      valueFrom:
        secretKeyRef:
          name: ${SECRET}
          key: username
EOF
${KUBECTL} -n ${NAMESPACE} wait --timeout=-1s --for=condition=Ready "pod/pgloader" | sed -Ee "s/^/ > ${KUBECTL} | /g"

echo "* Restoring database..." >&2
gzip -d -c ${SOURCE}/dump.tar.gz | ${KUBECTL} exec pgloader -n ${NAMESPACE} -i -- sh -c 'pg_restore --disable-triggers --clean --if-exists -F tar -d ${PGDATABASE}'

echo "* Cleaning up the pod..." >&2
${KUBECTL} delete pod pgloader -n ${NAMESPACE} --now | sed -Ee "s/^/ > ${KUBECTL} | /g"
${KUBECTL} -n ${NAMESPACE} wait --for="delete" --timeout=-1s pod pgloader | sed -e "s/^/ > ${KUBECTL} | /g"

echo "* done" >&2
