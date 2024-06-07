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
if [ -z "${NAMESPACE}" ]; then
  echo "NAMESPACE is not specified!" >&2
  exit 1
fi

PVC=${PVC:-}
if [ -z "${PVC}" ] ; then
  echo "PVC is not specified!" >&2
  exit 1
fi

DEST=${1}
if [ -z "${DEST}" ] ; then
  echo "Usage: ${0} <DEST_DIR>" >&2
  exit 1
fi

mkdir -p "${DEST}"

echo "* Backing up PVC ${PVC} from ${NAMESPACE} to ${DEST}" >&2

# stripping the bounded volume from the PVC
${KUBECTL} get pvc ${PVC} -n ${NAMESPACE} -o json | \
  ${JQ} 'del(.metadata.finalizers, .metadata.uid, .metadata.annotations, .metadata.resourceVersion, .spec.volumeName, .metadata.creationTimestamp, .status, .spec.volumeName, .spec.volumeMode)' \
  > ${DEST}/pvc.json

# download the volume
echo "* Downloading data from persistentvolumeclaim/${PVC}" >&2
echo "* Creating a pod named \"datadumper\"" >&2

cat <<EOF | ${KUBECTL} -n ${NAMESPACE} apply -f - | sed -Ee "s/^/ > ${KUBECTL} | /g"
apiVersion: v1
kind: Pod
metadata:
  name: datadumper
  namespace: "${NAMESPACE}"
spec:
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: "${PVC}"
  containers:
  - name: datadumper
    image: busybox:1.36.1
    command: ["sh", "-c", "sleep infinity"]
    volumeMounts:
    - name: data
      mountPath: /data
EOF

${KUBECTL} -n ${NAMESPACE} wait --timeout=-1s --for=condition=Ready pod/datadumper | sed -Ee "s/^/ > ${KUBECTL} | /g"

echo "* Transfarring storage content..." >&2

${KUBECTL} exec datadumper -n ${NAMESPACE} -- sh -c 'tar -C /data -czf - .' > ${DEST}/data.tar.gz
${KUBECTL} delete pod datadumper -n ${NAMESPACE} --wait --now | sed -Ee "s/^/ > ${KUBECTL} | /g"
echo "* done: ${PVC}" >&2
