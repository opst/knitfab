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

CHART_NAME=${CHART_NAME:-knit-image-registry}
if [ -z "${CHART_NAME}" ]; then
  echo "CHART_NAME is not specified!" >&2
  exit 1
fi

DEST=${1}
if [ -z "${DEST}" ] ; then
  echo "Usage: ${0} <DEST_DIR>" >&2
  exit 1
fi

PVC=$(${HELM} -n ${NAMESPACE} get values ${CHART_NAME} -o json -a | ${JQ} -r '.component')-registry-root

echo "* Backing up In-Cluster Image Registry from ${NAMESPACE} to ${DEST}" >&2
PVC=${PVC} ${HERE}/lib/pvc.sh "${DEST}"
