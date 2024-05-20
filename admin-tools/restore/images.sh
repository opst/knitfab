#! /bin/bash

HERE=$(cd ${0%/*}; pwd)

echo "*** Restore Images ***" >&2

KUBECTL=${KUBNECTL:-kubectl}
if [ -z "${KUBECTL}" ] ; then
  echo "KUBECTL is not specified!" >&2
  exit 1
fi

HELM=${HELM:-helm}
if [ -z "${HELM}" ] ; then
  echo "HELM is not specified!" >&2
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
export NAMESPACE

SOURCE=${1}
if [ -z "${SOURCE}" ] ; then
  echo "Usage: $0 <SOURCE_DIR>  # SOURCE_DIR is a directory which was ./backup-tool/backup/data DEST_DIR" >&2
  exit 1
fi

NEW_STORAGE_CLASS=${STORAGE_CLASS:- "(not changed)"}
export STORAGE_CLASS=${STORAGE_CLASS:-}

CLEAN=${CLEAN:-yes}
CLEAN_MODE=no
if [ -n "${CLEAN}" ] ; then
  CLEAN_MODE=yes
fi
export CLEAN


if ! [ -d "${SOURCE}" ] ; then
  echo "* Cannot restore from ${SOURCE} (not directory)" >&2
  exit 1
fi
if ! [ -r "${SOURCE}/pvc.json" ] || ! [ -r "${SOURCE}/data.tar.gz" ] ; then
  echo "* Cannot restore from ${SOURCE} (seems not a backup)" >&2
  exit 1
fi

cat <<EOF >&2
  - SOURCE: ${SOURCE}
  - NAMESPACE: ${NAMESPACE}
  - STORAGE CLASS: ${NEW_STORAGE_CLASS}
  - CLEAN: ${CLEAN_MODE}   # (If yes, delete the existing PVC for images before restoring.)
EOF

if [ -z "${IMAGE_REGISTRY_DEPLOYMENT}" ] ; then
  IMAGE_REGISTRY_DEPLOYMENT=$(${HELM} -n ${NAMESPACE} get values ${CHART_NAME:-knit-image-registry} -o json -a | ${JQ} -r '.component')-registry
fi


if [ -z "${FORCE}" ] ; then
  echo "" >&2
  read -p "Do you want to restore? [y/N]: " CONFIRM
  if ! [ "${CONFIRM}" = "y" ] ; then
    echo "Canceled." >&2
    exit 1
  fi
fi

REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deployment ${IMAGE_REGISTRY_DEPLOYMENT} -o json | ${JQ} -r '.spec.replicas')
echo "* Scaling ${IMAGE_REGISTRY_DEPLOYMENT} into 0" >&2
${KUBECTL} -n ${NAMESPACE} scale --replicas 0 deployment ${IMAGE_REGISTRY_DEPLOYMENT} \
  | sed -Ee "s/^/ > ${KUBECTL} | /g"

${HERE}/lib/pvc.sh ${SOURCE}

echo "* Scaling ${IMAGE_REGISTRY_DEPLOYMENT} into ${REPLICAS}" >&2
${KUBECTL} -n ${NAMESPACE} scale --replicas ${REPLICAS} deployment ${IMAGE_REGISTRY_DEPLOYMENT} \
  | sed -Ee "s/^/ > ${KUBECTL} | /g"
