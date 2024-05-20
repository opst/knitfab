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

SOURCE=${1}
if [ -z "${SOURCE}" ] ; then
  echo "Usage: $0 <SOURCE_DIR>" >&2
  exit 1
fi

echo "* Restoring PVC to ${NAMESPACE} from ${SOURCE}" >&2

PVC_JSON=${SOURCE}/pvc.json
if ! [ -r "${PVC_JSON}" ] ; then
  echo "PVC JSON file is not found!" >&2
  exit 1
fi

PVC_DATA=${SOURCE}/data.tar.gz
if ! [ -r "${PVC_DATA}" ] ; then
  echo "PVC data file is not found!" >&2
  exit 1
fi

PVC=$(${JQ} -r '.metadata.name' ${PVC_JSON})

if [ -n "${CLEAN}" ] ; then
  echo "* Deleting pvc/${PVC}" >&2
  ${KUBECTL} -n ${NAMESPACE} delete --now pvc ${PVC} | sed -e "s/^/ > ${KUBECTL} | /g"
  ${KUBECTL} -n ${NAMESPACE} wait --for="delete" --timeout=-1s pvc ${PVC} | sed -e "s/^/ > ${KUBECTL} | /g"
fi

if [ -z "${STORAGE_CLASS}" ] ; then
  STORAGE_CLASS=$(${JQ} -r '.spec.storageClassName' ${PVC_JSON})
fi

echo "* Registreing metadata persistentvolumeclaim/${PVC}" >&2
cat ${PVC_JSON} \
  | ${JQ} 'del(.spec.storageClass) + {"spec": ( .spec + {"storageClassName": "'${STORAGE_CLASS}'"})}' ${PVC_JSON} \
  | ${JQ} 'del(.metadata.namespace) + {"metadata": ( .metadata + {"namespace": "'${NAMESPACE}'"})}' ${PVC_JSON} \
  | ${KUBECTL} apply -n ${NAMESPACE} -f - \
  | sed -e "s/^/ > ${KUBECTL} | /g"

echo "* Loading storage content to persistentvolumeclaim/${PVC}" >&2

echo "* Creating a pod named \"dataloader\"" >&2
cat <<EOF | ${KUBECTL} -n ${NAMESPACE} apply --wait -f - | sed -e "s/^/ > ${KUBECTL} | /g"
apiVersion: v1
kind: Pod
metadata:
  name: dataloader
  namespace: ${NAMESPACE}
spec:
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: ${PVC}
  containers:
  - name: dataloader
    image: busybox:1.36.1
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: data
      mountPath: /data
EOF
${KUBECTL} -n ${NAMESPACE} wait --for="condition=Ready" --timeout=-1s pod dataloader | sed -e "s/^/ > ${KUBECTL} | /g"

echo "* Loading data to pvc/${PVC} ..." >&2
${KUBECTL} exec dataloader -n ${NAMESPACE} -i -- sh -c 'tar -C /data -xzf -' \
  < ${SOURCE}/data.tar.gz
${KUBECTL} delete pod dataloader -n ${NAMESPACE} --now --wait | sed -e "s/^/ > ${KUBECTL} | /g"
echo "* done." >&2
${KUBECTL} -n ${NAMESPACE} wait --for="delete" --timeout=-1s pod dataloader | sed -e "s/^/ > ${KUBECTL} | /g"
