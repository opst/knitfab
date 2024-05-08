#! /bin/bash
set -e

HERE=$(cd ${0%/*}; pwd)

HELM=${HELM:-helm}
if [ -z "${HELM}" ] ; then
  echo "HELM is not specified!" >&2
  exit 1
fi

export NAMESPACE=${NAMESPACE:-knitfab}
if [ -z "${NAMESPACE}" ]; then
  echo "NAMESPACE is not specified!" >&2
  exit 1
fi

CHART_NAME=${CHART_NAME:-knit-db-postgres}
if [ -z "${CHART_NAME}" ]; then
  echo "CHART_NAME is not specified!" >&2
  exit 1
fi

DEST=${1}
if [ -z "${DEST}" ] ; then
  echo "Usage: ${0} <DEST_DIR>" >&2
  exit 1
fi


if [ -z "${PGHOST}" ] ; then
  PGHOST=$(${HELM} -n ${NAMESPACE} get values ${CHART_NAME} -o json -a | jq -r '.service')
fi
export PGHOST

if [ -z "${SECRET}" ] ; then
  SECRET=$(${HELM} -n ${NAMESPACE} get values ${CHART_NAME} -o json -a | jq -r '.credential.secret')
fi
export SECRET

${HERE}/lib/postgres.sh ${DEST}
