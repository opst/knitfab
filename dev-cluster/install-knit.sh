#!/bin/bash
set -e

# Install Knitfab in a local Kubernetes cluster using Minikube.
#
# This script is intended to be run in a local development environment.
# To setup your minikube cluster, use the `up-mini.sh` script.

HERE=$(cd ${0%/*}; pwd)
ROOT=$(cd ${HERE}/../; pwd)
export KUBECONTEXT=${KUBECONTEXT:-$(cat ${HERE}/.dev-cluster-kube-context)}
export HELM_PLUGINS=${HERE}/helm-plugins
export NAMESPACE=${NAMESPACE:-knit-dev}

ARGS=
BUILD=1
while [ -n "${1}" ] ; do
	ARG=${1}; shift
	case ${ARG} in
		--prepare)
			PREPARE=true
			BUILD=
			;;
		--renew-certs)
			BUILD=
			ARGS="${ARGS} --renew-certs"
			;;
		*)
			ARGS="${ARGS} ${ARG}"
			;;
	esac
done

if [ -n "${PREPARE}" ] ; then
	${ROOT}/installer/installer.sh --prepare -s ${HERE}/knitfab-install-settings ${ARGS}
	exit
fi

if [ -n "${BUILD}" ] ; then
	${ROOT}/build/build.sh
	NODE_IP=$(minikube -p ${KUBECONTEXT} ip)
	IMAGE_REGISTRY="${NODE_IP}:30005" bash ${ROOT}/bin/images/local/publish.sh
	# portnumber comes from ./lib/install-registry.sh.
fi

export CHART_VERSION=$(cat ${ROOT}/charts/local/CHART_VERSION)
export IMAGE_REPOSITORY_HOST="localhost:30005"
export REPOSITORY=local
export CHART_REPOSITORY_ROOT="file://${ROOT}/charts/local"
${ROOT}/installer/installer.sh --install -s ${HERE}/knitfab-install-settings --version ${VERSION} ${ARGS}
helm repo remove knitfab
