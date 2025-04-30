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

# detect IP addr of this machine
if [ -z "${IP}" ] ; then
  if which ip > /dev/null 2>&1 ; then
    IP=$(ip -4 addr show | grep -E 'inet (10|192|172)' | awk '{print $2}' | cut -d/ -f1 | head -n 1)
  elif which ifconfig > /dev/null 2>&1 ; then
    IP=$(ifconfig | grep -E 'inet (10|192|172)' | awk '{print $2}' | head -n 1)
  else 
    echo "Cannot find IP address of this machine. Please set IP manually."
    exit 1
  fi
fi

if [ -n "${PREPARE}" ] ; then
	${ROOT}/installer/installer.sh --prepare -s ${HERE}/knitfab-install-settings --tls-cert-ip ${IP} ${ARGS}
	exit
fi


if [ -n "${BUILD}" ] ; then
	${ROOT}/build/build.sh
	
	(
		echo "start port-forwarding"
		kubectl --context ${KUBECONTEXT} -n registry port-forward --address ${IP} service/image 30005:5000 > /dev/null &
		PID_PORTFWD=$!
		trap "kill ${PID_PORTFWD}; echo stop port-forwarding" EXIT
		sleep 5
		IMAGE_REGISTRY="${IP}:30005" bash ${ROOT}/bin/images/local/publish.sh
		# portnumber comes from ./lib/install-registry.sh.
	)
fi

export CHART_VERSION=$(cat ${ROOT}/charts/local/CHART_VERSION)
export IMAGE_REPOSITORY_HOST="localhost:30005"
export REPOSITORY=local
export CHART_REPOSITORY_ROOT="file://${ROOT}/charts/local"
${ROOT}/installer/installer.sh --install -s ${HERE}/knitfab-install-settings --version ${VERSION} ${ARGS}
helm repo remove knitfab
