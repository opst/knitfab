#! /bin/bash

HERE=$(cd ${0%/*}; pwd)

echo "*** Restore Knitfab Data ***" >&2

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

SOURCE=${1}
if [ -z "${SOURCE}" ] ; then
  echo "Usage: $0 <SOURCE_DIR>  # SOURCE_DIR is a directory which was ./backup-tool/backup/data DEST_DIR" >&2
  exit 1
fi

NEW_STORAGE_CLASS=${STORAGE_CLASS:- "(not changed)"}
export STORAGE_CLASS=${STORAGE_CLASS:-}

CLEAN=${CLEAN:-}
CLEAN_MODE=no
if [ -n "${CLEAN}" ] ; then
  CLEAN_MODE=yes
fi
export CLEAN

cat <<EOF >&2
  - SOURCE: ${SOURCE}/*  # Each directory containing pvc.json and data.tar.gz
  - NAMESPACE: ${NAMESPACE}
  - STORAGE CLASS: ${NEW_STORAGE_CLASS}
  - CLEAN: ${CLEAN_MODE}   # If yes, delete existing PVCs for Knitfab Data before restoring.
EOF

if [ -z "${FORCE}" ] ; then
  echo "" >&2
  read -p "Do you want to restore? [y/N]: " CONFIRM
  if ! [ "${CONFIRM}" = "y" ] ; then
    echo "Canceled." >&2
    exit 1
  fi
fi

for D in ${SOURCE}/* ; do
  if ! [ -d "${D}" ] ; then
    echo "* Skip ${D} (not directory)" >&2
    continue
  fi
  if ! [ -r "${D}/pvc.json" ] || ! [ -r "${D}/data.tar.gz" ] ; then
    echo "* Skip ${D} (seems not a backup)" >&2
    continue
  fi

  ${HERE}/lib/pvc.sh ${D}
done
