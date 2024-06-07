#! /bin/sh

HERE=$(cd ${0%/*}; pwd)

echo "*** Restore database ***" >&2

HELM=${HELM:-helm}
if [ -z "${HELM}" ] ; then
  echo "HELM is not specified!" >&2
  exit 1
fi

NAMESPACE=${NAMESPACE:-knitfab}
if [ -z "${NAMESPACE}" ]; then
  echo "NAMESPACE is not specified!" >&2
  exit 1
fi
export NAMESPACE

CHART_NAME=${CHART_NAME:-knit-db-postgres}
if [ -z "${CHART_NAME}" ]; then
  echo "CHART_NAME is not specified!" >&2
  exit 1
fi

SOURCE=${1}
if [ -z "${SOURCE}" ] ; then
  echo "Usage: $0 <SOURCE_DIR>  # SOURCE_DIR is a directory which was ./backup-tool/backup/db DUMP_DIR" >&2
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

cat <<EOF >&2
  - SOURCE: ${SOURCE}
  - NAMESPACE: ${NAMESPACE}
  - DATABASE SERVICE NAME: ${PGHOST}
EOF

if [ -z "${FORCE}" ] ; then
  echo "" >&2
  read -p "Do you want to restore? [y/N]: " CONFIRM
  if ! [ "${CONFIRM}" = "y" ] ; then
    echo "Canceled." >&2
    exit 1
  fi
fi

${HERE}/lib/postgres.sh ${SOURCE}
