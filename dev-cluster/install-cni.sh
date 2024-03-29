#! /bin/bash

cd ${0%/*}
export KUBECONFIG="./.sync/kubeconfig/kubeconfig"

HELM=${HELM:-helm}

if ${HELM} status calico > /dev/null 2>&1 ; then
	exit
fi

${HELM} repo add projectcalico https://projectcalico.docs.tigera.io/charts
${HELM} repo update projectcalico
${HELM} install calico projectcalico/tigera-operator --version v3.27.2 --wait
