#! /bin/bash
set -e

HERE=${0%/*}
ROOT=$(cd ${HERE}/../; pwd)

HELM=${HELM:-helm}
KUBECTL=${KUBECTL:-kubectl}

BIN=${ROOT}/bin/installer/bin
IMAGES=${ROOT}/bin/images
CHARTS=${ROOT}/charts/local

export KUBECONFIG=${KUBECONFIG:-${HERE}/.sync/kubeconfig/kubeconfig}
NAMESPACE=${NAMESPACE:-knit-dev}

${HELM} uninstall -n ${NAMESPACE} knit-app || :
${HELM} uninstall -n ${NAMESPACE} knit-schema-upgrader || :
${HELM} uninstall -n ${NAMESPACE} knit-db-postgres || :
${HELM} uninstall -n ${NAMESPACE} knit-certs || :
${HELM} uninstall -n ${NAMESPACE} knit-image-registry || :

${KUBECTL} -n ${NAMESPACE} delete pvc --all

sleep 30

${HELM} -n ${NAMESPACE} uninstall knit-storage-nfs || :
