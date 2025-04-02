#! /bin/bash
set -e
HERE=$(cd ${0%/*}; pwd)
MINIKUBE_PROFILE=${MINIKUBE_PROFILE:-$(cat "${HERE}/.dev-cluster-kube-context")}
minikube -p ${MINIKUBE_PROFILE} delete
