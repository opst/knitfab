#! /bin/bash
set -e

HERE=${0%/*}
KUBECTL=${KUBECTL:-kubectl}

KUBECONFIG=${KUBECONFIG:-${HERE}/.sync/kubeconfig/kubeconfig}
export KUBECONFIG

exec ${KUBECTL} $@
