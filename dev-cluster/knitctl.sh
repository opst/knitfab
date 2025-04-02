#! /bin/bash
set -e

HERE=${0%/*}
KUBECTL=${KUBECTL:-kubectl}

KUBECONTEXT=${KUBECONTEXT:-$(cat "${HERE}/.dev-cluster-kube-context")}

exec ${KUBECTL} --context "${KUBECONTEXT}" $@
