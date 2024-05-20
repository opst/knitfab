#! /bin/bash
set -e

HERE=$(cd ${0%/*}; pwd)

HELM=${HELM:-helm}
if [ -z "${HELM}" ] ; then
  echo "HELM is not specified!" >&2
  exit 1
fi
KUBECTL=${KUBECTL:-kubectl}
if [ -z "${KUBECTL}" ] ; then
  echo "KUBECTL is not specified!" >&2
  exit 1
fi
JQ=${JQ:-jq}

export NAMESPACE=${NAMESPACE:-knitfab}
if [ -z "${NAMESPACE}" ]; then
  echo "NAMESPACE is not specified!" >&2
  exit 1
fi

CHART_NAME=${CHART_NAME:-knit-app}
if [ -z "${CHART_NAME}" ]; then
  echo "CHART_NAME is not specified!" >&2
  exit 1
fi

DEST=${1}
if [ -z "${DEST}" ] ; then
  echo "Usage: ${0} <DEST_DIR>" >&2
  exit 1
fi

echo "* Backing up Knitfab Data from ${NAMESPACE} to ${DEST}/\${PVC} ..." >&2
STORAGE_CLASS=$(${HELM} -n ${NAMESPACE} get values ${CHART_NAME} -o json -a | ${JQ} -r '.storage.class.data')
for PVC in $(${KUBECTL} get pvc -n ${NAMESPACE} -o json | ${JQ} -r '.items[] | select(.spec.storageClassName == "'${STORAGE_CLASS}'") | .metadata.name') ; do
  PVC=${PVC} ${HERE}/lib/pvc.sh "${DEST}/${PVC}"
done
