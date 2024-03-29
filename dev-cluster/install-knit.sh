#! /bin/bash
set -e

HERE=${0%/*}
ROOT=$(cd ${HERE}/../; pwd)

HELM=${HELM:-helm}
DOCKER=${DOCKER:-docker}

BIN=${ROOT}/bin/installer/bin
CHARTS=${ROOT}/charts/local
CHART_VERSION=$(cat ${CHARTS}/CHART_VERSION)
HANDOUT=${HANDOUT:-${ROOT}/configure/handout}

DEVCLUSTER_KUBECONFIG="${HERE}/.sync/kubeconfig/kubeconfig"
export KUBECONFIG="${KUBECONFIG:-${DEVCLUSTER_KUBECONFIG}}"
KUBECTL=${KUBECTL:-kubectl}
NAMESPACE=${NAMESPACE:-knit-dev}

KNIT_REGISTRY_PORT=${KNIT_REGISTRY_PORT:-30503}
KNIT_API_PORT=${KNIT_API_PORT:-30803}

# ------

function get_node_ip() {
	${KUBECTL} get nodes -o jsonpath='{.items[*].status.addresses[?(@.type=="InternalIP")].address}'
}

if [ "${KUBECONFIG}" = "${DEVCLUSTER_KUBECONFIG}" ] ; then
	${HERE}/install-cni.sh
	. ${HERE}/.sync/knit/install_setting
else
	CERTSDIR=${CERTSDIR:-${HERE}/.sync/certs}
	if [ -d "${CERTSDIR}" ] ; then
		TLSCACERT=${CERTSDIR}/ca.crt
		TLSCAKEY=${CERTSDIR}/ca.key
		TLSCERT=${CERTSDIR}/server.crt
		TLSKEY=${CERTSDIR}/server.key
	fi
fi

if [ -z "${KNIT_REGISTRY_HOST}" ] ; then
	for NAME in $(get_node_ip) ; do
		KNIT_REGISTRY_HOST=${NAME}
		break
	done
fi
if [ -z "${KNIT_API_HOST}" ] ; then
	for NAME in $(get_node_ip) ; do
		KNIT_API_HOST=${NAME}
		break
	done
fi

function message() {
	echo "$@" >&2
}

function run() {
	echo '$ '"${@: 0:3} ... ${@: -2:2}" >&2
	# echo '$ '"$@" >&2
	"$@"
}

# ------

MODE=install
for ARG in ${@} ; do
	case ${ARG} in
		--upgrade)
			MODE=upgrade
			;;
	esac
done

message "preparing..."

mkdir -p ${HANDOUT}/docker/certs.d/${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT}
cp -r ${TLSCACERT} ${ROOT}/configure/
cp -r ${TLSCACERT} ${HANDOUT}/docker/certs.d/${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT}/

cat <<EOF > ${HANDOUT}/knitprofile
apiRoot: https://${KNIT_API_HOST}:${KNIT_API_PORT}/api
cert:
  ca: $(cat ${TLSCACERT} | base64 | tr -d '\r\n')
EOF

# ------

message "phase 1/4: ${MODE} knit storage driver"

message ""
message "------"
if [ "${MODE}" = "install" ] && ${HELM} status knit-storage-nfs -n ${NAMESPACE} > /dev/null 2> /dev/null ; then
	message "already installed: knit-storage-nfs"
elif [ -n "${KNIT_NFS_HOST}" ] ; then
	run ${HELM} ${MODE} --dependency-update --wait \
		-n ${NAMESPACE} --create-namespace \
		--set "nfs.external=true" \
		--set "nfs.server=${KNIT_NFS_HOST}" \
		--set "nfs.share=${KNIT_NFS_EXPORT}" \
		--set-json "nfs.mountOptions=\"${KNIT_NFS_MOUNTOPTIONS}\"" \
		knit-storage-nfs ${CHARTS}/${CHART_VERSION}/knit-storage-nfs
	sleep 5
else
	message "NFS: set up with in-cluster mode"
	run ${HELM} ${MODE} --dependency-update --wait \
		-n ${NAMESPACE} --create-namespace \
		--set "nfs.external=false" \
		--set "nfs.node=" \
		--set "nfs.hostPath=" \
		knit-storage-nfs ${CHARTS}/${CHART_VERSION}/knit-storage-nfs
	sleep 5
fi
message "======"
message ""
message "phase 1/4: OK."
message ""

# # # # # # PHASE 2 # # # # # #
message "phase 2/4: ${MODE} knit middlewares"

message ""
message "------"
if [ "${MODE}" = "install" ] && ${HELM} status knit-certs -n ${NAMESPACE} > /dev/null 2> /dev/null ; then
	message "already installed: knit-certs"
else
	run ${HELM} ${MODE} --dependency-update --wait \
		-n ${NAMESPACE} --create-namespace \
		--set "cert=$(cat ${TLSCERT} | base64 | tr -d '\r\n')" \
		--set "key=$(cat ${TLSKEY} | base64 | tr -d '\r\n')" \
		--set "cacert=$(cat ${TLSCACERT} | base64 | tr -d '\r\n')" \
		--set "cakey=$(cat ${TLSCAKEY} | base64 | tr -d '\r\n')" \
		knit-certs ${CHARTS}/${CHART_VERSION}/knit-certs
fi
sleep 5
message "======"
message ""

KNIT_RDB_PASSWORD=test-pass
message ""
message "------"
if [ "${MODE}" = "install" ] && ${HELM} status knit-db-postgres -n ${NAMESPACE} > /dev/null 2> /dev/null ; then
	message "already installed: knit-db-postgres"
else
	run ${HELM} ${MODE} --dependency-update --wait \
		-n ${NAMESPACE} --create-namespace \
		--set-json "storage=$(${HELM} get values knit-storage-nfs -n ${NAMESPACE} -o json --all)" \
		--set "credential.username=${KNIT_RDB_USERNAME:-knit}" \
		--set "credential.password=${KNIT_RDB_PASSWORD}" \
	knit-db-postgres ${CHARTS}/${CHART_VERSION}/knit-db-postgres
fi
message "======"
message ""


message ""
message "------"
if [ "${MODE}" = "install" ] && ${HELM} status knit-image-registry -n ${NAMESPACE} > /dev/null 2> /dev/null ; then
	message "already installed: knit-image-registry"
else
	run ${HELM} ${MODE} --dependency-update --wait \
		-n ${NAMESPACE} --create-namespace \
		--set "server=${KNIT_REGISTRY_HOST}" \
		--set "port=${KNIT_REGISTRY_PORT}" \
		--set-json "storage=$(${HELM} -n ${NAMESPACE} get values -o json --all knit-storage-nfs)" \
		--set-json "certs=$(${HELM} -n ${NAMESPACE} get values -o json --all knit-certs)" \
		knit-image-registry ${CHARTS}/${CHART_VERSION}/knit-image-registry
fi
message "======"
message ""

if curl -f -s --cacert ${TLSCACERT} "https://${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT}" > /dev/null ; then
	:
else
	message "waiting docker image repository get up..."
	while ! curl -f -s --cacert ${TLSCACERT} "https://${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT}" > /dev/null ; do
		message -n "."
		sleep 2
	done
	message ""
fi
sleep 10

message "phase 2/4: OK."
message ""

# # # # # # PHASE 3 # # # # # #
message "phase 3/4: push knit images"

# FIXME: image registry should have authentiaction.
cat ${CHARTS}/images/index | \
while read APP TAG ; do
	run ${DOCKER} tag ${TAG} ${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT}/${TAG}
	if run ${DOCKER} push ${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT}/${TAG} ; then
		:
	else
		message "retry..."
		sleep 10
		if run ${DOCKER} push ${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT}/${TAG} ; then
			:
		else
			message "failed to push image: ${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT}/${TAG}"
			message "(have you put ${TLSCACERT} into /etc/docker/certs.d/${KNIT_REGISTRY_HOST}:${KNIT_REGISTRY_PORT} as ca.crt ?)"
			exit 1
		fi
	fi
done

message ""
message "phase 3/4: OK."
message ""

# # # # # # PHASE 4 # # # # # #
message "phase 4/4: ${MODE} knit app"
message ""
message "------"
if [ "${MODE}" = "install" ] && ${HELM} status knit-app -n ${NAMESPACE} > /dev/null 2> /dev/null ; then
	message "already installed: knit-app"
else
	run ${HELM} ${MODE} --dependency-update --wait \
		--create-namespace -n ${NAMESPACE} \
		--set-json "storage=$(${HELM} -n ${NAMESPACE} get values -o json --all knit-storage-nfs)" \
		--set-json "certs=$(${HELM} -n ${NAMESPACE} get values -o json --all knit-certs)" \
		--set-json "database=$(${HELM} -n ${NAMESPACE} get values -o json --all knit-db-postgres)" \
		--set-json "registry=$(${HELM} -n ${NAMESPACE} get values -o json --all knit-image-registry)" \
		--set "clusterTLD=cluster.local" \
		--set "imageRepository=localhost:${KNIT_REGISTRY_PORT}" \
		--set "knitd.port=${KNIT_API_PORT}" \
		--set "knitd_backend.logLevel=debug" \
		knit-app ${CHARTS}/${CHART_VERSION}/knit-app
fi
message "======"
message ""
message "phase 4/4: OK."
message ""

message "Applied Helm Releases:"
message ""
message " * knit-storage-nfs    : NFS bases storage driver for knitfab"
message " * knit-certs          : TLS Certifications for knitfab"
message " * knit-image-registry : in-cluster image registry"
message " * knit-db-postgres    : database for knitfab"
message " * knit-app            : knitfab api and backend services"
message ""
${HELM} list -n ${NAMESPACE} | sed 's/^/> /'
message ""
