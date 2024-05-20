#! /bin/bash
set -e

KUBECTL=${KUBECTL:-kubectl}
if ! command -v ${KUBECTL} >/dev/null 2>&1; then
	echo "kubectl (${KUBECTL}) not found" >&2
	exit 1
fi

HELM=${HELM:-helm}
if ! command -v ${HELM} >/dev/null 2>&1; then
	echo "helm (${HELM}) not found" >&2
	exit 1
fi

KNIT=${KNIT:-knit}
if ! command -v ${KNIT} >/dev/null 2>&1; then
	echo "knit (${KNIT}) not found" >&2
	exit 1
fi

JQ=${JQ:-jq}
if ! command -v ${JQ} >/dev/null 2>&1; then
	echo "jq (${JQ}) not found" >&2
	exit 1
fi

NAMESPACE=${NAMESPACE:-}
if [ -z "${NAMESPACE}" ]; then
	echo "NAMESPACE not set" >&2
	exit 1
fi

CHART_NAME_APP=${CHART_NAME_APP:-knit-app}
if [ -z "${CHART_NAME_APP}" ]; then
	echo "CHART_NAME_APP not set" >&2
	exit 1
fi

CHART_NAME_IMAGE_REGISTRY=${CHART_NAME_IMAGE_REGISTRY:-knit-image-registry}
if [ -z "${CHART_NAME_IMAGE_REGISTRY}" ]; then
	echo "CHART_NAME_IMAGE_REGISTRY not set" >&2
	exit 1
fi

echo "* Analyzing Knitfab components" >&2

if [ "0" = "$(${HELM} -n ${NAMESPACE} list --filter ${CHART_NAME_APP} -o json | ${JQ} -r '. | length')" ] ; then
	echo "${CHART_NAME_APP} is not detected. Have you installed Knitfab in namespace ${NAMESPACE}?" >&2
	exit 1
fi
echo " - ${CHART_NAME_APP} is detected." >&2

KNIT_KNITD=${KNIT_KNITD:-}
if [ -z "${KNIT_KNITD}" ]; then
	KNIT_KNITD=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_APP}" -o json | ${JQ} -r '.knitd.component')
fi
if [ -z "${KNIT_KNITD}" ]; then
	echo "KNIT_KNITD not set" >&2
	exit 1
fi
KNIT_KNITD_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_KNITD} -o json | ${JQ} -r '.spec.replicas')

KNIT_KNITD_BACKEND=${KNIT_KNITD_BACKEND:-}
if [ -z "${KNIT_KNITD_BACKEND}" ]; then
	KNIT_KNITD_BACKEND=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_APP}" -o json | ${JQ} -r '.knitd_backend.component')
fi
if [ -z "${KNIT_KNITD_BACKEND}" ]; then
	echo "KNIT_KNITD_BACKEND not set" >&2
	exit 1
fi
KNIT_KNITD_BACKEND_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_KNITD_BACKEND} -o json | ${JQ} -r '.spec.replicas')

KNIT_PROJECTION=${KNIT_PROJECTION:-}
if [ -z "${KNIT_PROJECTION}" ]; then
	KNIT_PROJECTION=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_APP}" -o json | ${JQ} -r '.loops.projection.component')-leader
fi
if [ -z "${KNIT_PROJECTION}" ]; then
	echo "KNIT_PROJECTION not set" >&2
	exit 1
fi
KNIT_PROJECTION_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_PROJECTION} -o json | ${JQ} -r '.spec.replicas')

KNIT_INITIALIZE=${KNIT_INITIALIZE:-}
if [ -z "${KNIT_INITIALIZE}" ]; then
	KNIT_INITIALIZE=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_APP}" -o json | ${JQ} -r '.loops.initialize.component')-leader
fi
if [ -z "${KNIT_INITIALIZE}" ]; then
	echo "KNIT_INITIALIZE not set" >&2
	exit 1
fi
KNIT_INITIALIZE_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_INITIALIZE} -o json | ${JQ} -r '.spec.replicas')

KNIT_RUN_MANAGEMENT=${KNIT_RUN_MANAGEMENT:-}
if [ -z "${KNIT_RUN_MANAGEMENT}" ]; then
	KNIT_RUN_MANAGEMENT=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_APP}" -o json | ${JQ} -r '.loops.run_management.component')-leader
fi
if [ -z "${KNIT_RUN_MANAGEMENT}" ]; then
	echo "KNIT_RUN_MANAGEMENT not set" >&2
	exit 1
fi
KNIT_RUN_MANAGEMENT_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_RUN_MANAGEMENT} -o json | ${JQ} -r '.spec.replicas')

KNIT_HOUSEKEEPING=${KNIT_HOUSEKEEPING:-}
if [ -z "${KNIT_HOUSEKEEPING}" ]; then
	KNIT_HOUSEKEEPING=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_APP}" -o json | ${JQ} -r '.loops.housekeeping.component')-leader
fi
if [ -z "${KNIT_HOUSEKEEPING}" ]; then
	echo "KNIT_HOUSEKEEPING not set" >&2
	exit 1
fi
KNIT_HOUSEKEEPING_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_HOUSEKEEPING} -o json | ${JQ} -r '.spec.replicas')

KNIT_FINISHING=${KNIT_FINISHING:-}
if [ -z "${KNIT_FINISHING}" ]; then
	KNIT_FINISHING=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_APP}" -o json | ${JQ} -r '.loops.finishing.component')-leader
fi
if [ -z "${KNIT_FINISHING}" ]; then
	echo "KNIT_FINISHING not set" >&2
	exit 1
fi
KNIT_FINISHING_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_FINISHING} -o json | ${JQ} -r '.spec.replicas')

KNIT_GARBAGE_COLLECTION=${KNIT_GARBAGE_COLLECTION:-}
if [ -z "${KNIT_GARBAGE_COLLECTOR}" ]; then
	KNIT_GARBAGE_COLLECTION=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_APP}" -o json | ${JQ} -r '.loops.gc.component')-leader
fi
if [ -z "${KNIT_GARBAGE_COLLECTION}" ]; then
	echo "KNIT_GARBAGE_COLLECTION not set" >&2
	exit 1
fi
KNIT_GARBAGE_COLLECTION_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_GARBAGE_COLLECTION} -o json | ${JQ} -r '.spec.replicas')

if ! [ "0" = $(${HELM} -n ${NAMESPACE} list --filter "${CHART_NAME_IMAGE_REGISTRY}" -o json | ${JQ} -r '. | length') ] ; then
	echo " - ${CHART_NAME_IMAGE_REGISTRY} is detected." >&2
	KNIT_IMAGE_REGISTRY=${KNIT_IMAGE_REGISTRY:-}
	if [ -z "${KNIT_IMAGE_REGISTRY}" ]; then
		KNIT_IMAGE_REGISTRY=$(${HELM} -n ${NAMESPACE} get values -a "${CHART_NAME_IMAGE_REGISTRY}" -o json | ${JQ} -r '.component')-registry
	fi
	if [ -z "${KNIT_IMAGE_REGISTRY}" ]; then
		echo "KNIT_IMAGE_REGISTRY not set" >&2
		exit 1
	fi
	KNIT_IMAGE_REGISTRY_REPLICAS=$(${KUBECTL} -n ${NAMESPACE} get deploy ${KNIT_IMAGE_REGISTRY} -o json | ${JQ} -r '.spec.replicas')
fi


echo "* Freezing Knitfab in namespace ${NAMESPACE}" >&2
# knitd-backend: to reject uploading new Data
# garbage-collection-leader: to stop garbage collection, deleting PVC
# initialize-leader, projection-leader: to stop initializing new runs
(
	${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=0 \
		${KNIT_KNITD_BACKEND} \
		${KNIT_GARBAGE_COLLECTION} \
		${KNIT_INITIALIZE} \
		${KNIT_PROJECTION} \
		${KNIT_IMAGE_REGISTRY}  # if any.
) | sed "s/^/ > ${KUBECTL} | /"

echo "* Force stopping all runs..." >&2
for RUN_ID in $(${KNIT} run find -s ready -s starting -s running | ${JQ} -r '.[] | select( .plan.image ) | .runId'); do
	echo "* Stopping Run ${RUN_ID}" >&2
	${KNIT} run stop --fail ${RUN_ID}
done

echo "* Waiting for all runs to finish" >&2
while [ "0" != "$(${KNIT} run find -s ready -s starting -s running -s aborting -s completing | jq 'length')" ] ; do
	sleep 5
done

# stop all deployments in knit-app.
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=0 \
	${KNIT_KNITD} \
	${KNIT_RUN_MANAGEMENT} \
	${KNIT_HOUSEKEEPING} \
	${KNIT_FINISHING} \

echo "* Knitfab is frozen." >&2
echo "" >&2
echo "* To unfreeze, run:" >&2
echo "" >&2
echo "==================" >&2
cat <<EOF | tee system-unfreeze.sh
#! /bin/bash
set -e

#
# Unfreezing Script  -- generated by system-freeze.sh at $(date)
#
# You may need to set environmental variable "KUBECONFIG" to access the cluster.
#

${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_KNITD_REPLICAS} ${KNIT_KNITD}
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_KNITD_BACKEND_REPLICAS} ${KNIT_KNITD_BACKEND}
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_PROJECTION_REPLICAS} ${KNIT_PROJECTION}
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_INITIALIZE_REPLICAS} ${KNIT_INITIALIZE}
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_RUN_MANAGEMENT_REPLICAS} ${KNIT_RUN_MANAGEMENT}
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_HOUSEKEEPING_REPLICAS} ${KNIT_HOUSEKEEPING}
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_FINISHING_REPLICAS} ${KNIT_FINISHING}
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_GARBAGE_COLLECTION_REPLICAS} ${KNIT_GARBAGE_COLLECTION}
${KUBECTL} -n ${NAMESPACE} scale deployment --replicas=${KNIT_IMAGE_REGISTRY_REPLICAS} ${KNIT_IMAGE_REGISTRY}

EOF

chmod +x system-unfreeze.sh
echo "==================" >&2
echo "(this script is saved as system-unfreeze.sh)"
