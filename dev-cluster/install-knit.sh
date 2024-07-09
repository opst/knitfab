#! /bin/bash
set -e

HERE=$(cd ${0%/*}; pwd)
ROOT=$(cd ${HERE}/../; pwd)
JQ=${JQ:-jq}
HELM=${HELM:-helm}
KUBECTL=${KUBECTL:-kubectl}
export KUBECONFIG=${KUBECONFIG:-${HERE}/.sync/kubeconfig/kubeconfig}
export NAMESPACE=${NAMESPACE:-knit-dev}
export HELM_PLUGINS=${HERE}/helm-plugins

ARGS=
while [ -n "${1}" ] ; do
	ARG=${1}; shift
	case ${ARG} in
		--prepare)
			PREPARE=true
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

${ROOT}/build/build.sh

NODE_IP=$(${KUBECTL} get node -o json knit-gateway | ${JQ} -r '.status.addresses[] | select(.type == "InternalIP") | .address')
IMAGE_REGISTRY="${NODE_IP}:30005" bash ${ROOT}/bin/images/local/publish.sh
# portnumber comes from ./install-registry.sh.

export CHART_VERSION=$(cat ${ROOT}/charts/local/CHART_VERSION)
export IMAGE_REPOSITORY_HOST="localhost:30005"
export REPOSITORY=local
export CHART_REPOSITORY_ROOT="file://${ROOT}/charts/local"
${ROOT}/installer/installer.sh --install -s ${HERE}/knitfab-install-settings --version ${VERSION} ${ARGS}
${HELM} repo remove knitfab
