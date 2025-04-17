#! /bin/bash
set -e

HELM=${HELM:-helm}
KUBECTL=${KUBECTL:-kubectl}
MINIKUBE=${MINIKUBE:-minikube}
COLIMA=${COLIMA:-colima}

KUBECONFIG=${KUBECONFIG:-${HOME}/.kube/config}

cd ${0%/*}
if [ -r ./testenv ] ; then
	source ./testenv
fi

KNIT_TEST_KUBECONFIG=${KNIT_TEST_KUBECONFIG:-${KUBECONFIG}}
KNIT_TEST_COLIMA_PROFILE=${KNIT_TEST_COLIMA_PROFILE:-knit-test}
KNIT_TEST_KUBECTX=${KNIT_TEST_KUBECTX:-colima-${KNIT_TEST_COLIMA_PROFILE}}
KNIT_TEST_NAMESPACE=${NAMESPACE:-knit-test}

BASE_KUBECONTEXT=${BASE_KUBECONTEXT:-$(kubectl --kubeconfig ${KNIT_TEST_KUBECONFIG} config current-context)}
trap "kubectl config use-context ${BASE_KUBECONTEXT}" EXIT

# start up k8s and write envvars for docker
function up() {
	case ${1} in
		colima|*)  # default
			if ! colima status -p ${KNIT_TEST_COLIMA_PROFILE} 2> /dev/null ; then
				${COLIMA} start -p ${KNIT_TEST_COLIMA_PROFILE} --kubernetes
			fi
		;;
	esac
}
# echo "no kubeconfig supplied. use minikube (with command '${MINIKUBE}') ." >&2

HELMOPTS="--kubeconfig ${KNIT_TEST_KUBECONFIG} --kube-context ${KNIT_TEST_KUBECTX}"
KUBEOPTS="--kubeconfig ${KNIT_TEST_KUBECONFIG} --context ${KNIT_TEST_KUBECTX}"

case ${1} in
	install)
		up ${2}
		APP_VERSION=TEST CHART_VERSION=v0.0.0 ARCH=test ./build/build.sh --test image chart
		echo "" >&2

		${HELM} ${HELMOPTS} install --namespace ${KNIT_TEST_NAMESPACE} --create-namespace --dependency-update --wait \
			--set "nfs.node=" --set "nfs.hostPath=" \
			--set "csi-driver-nfs.controller.replicas=1" \
			knit-storage-nfs "./charts/test/v0.0.0/knit-storage-nfs/"

		echo "" >&2

		${HELM} ${HELMOPTS} install --namespace ${KNIT_TEST_NAMESPACE} --create-namespace --dependency-update --wait \
			--set-json "ephemeral=true" \
			--set-json "storage=$(${HELM} ${HELMOPTS} --namespace ${KNIT_TEST_NAMESPACE} get values -o json --all knit-storage-nfs)" \
			--set-json 'credential={"secret":"database-credential","username":"test-user","password":"test-pass"}' \
			knit-db-postgres "./charts/test/v0.0.0/knit-db-postgres/"

		echo "" >&2

		${HELM} ${HELMOPTS} install --namespace ${KNIT_TEST_NAMESPACE} --create-namespace --dependency-update --wait \
			--set-json "storage=$(${HELM} ${HELMOPTS} --namespace ${KNIT_TEST_NAMESPACE} get values -o json --all knit-storage-nfs)" \
			--set-json "database=$(${HELM} ${HELMOPTS} --namespace ${KNIT_TEST_NAMESPACE} get values -o json --all knit-db-postgres)" \
			knit-schema-upgrader ./charts/test/v0.0.0/knit-schema-upgrader

		echo "" >&2

		${HELM} ${HELMOPTS} install --namespace ${KNIT_TEST_NAMESPACE} --create-namespace --dependency-update --wait \
			--set-json "storage=$(${HELM} ${HELMOPTS} --namespace ${KNIT_TEST_NAMESPACE} get values -o json --all knit-storage-nfs)" \
			knit-test ./charts/src/knit-test

		echo "" >&2

		${KUBECTL} ${KUBEOPTS} -n knit-test get pods
		echo "" >&2
		echo "your test environment has started!" >&2
		echo "---------" >&2
		echo "# Knit test environment. Generated with testctl.sh" > ./.testenv
		echo "" >> ./.testenv
		echo "KNIT_TEST_KUBECONFIG=${KNIT_TEST_KUBECONFIG}" | tee -a ./.testenv >&2
		echo "KNIT_TEST_COIIMA_PROFILE=${KNIT_TEST_COLIMA_PROFILE}" | tee -a ./.testenv >&2
		echo "KNIT_TEST_KUBECTX=${KNIT_TEST_KUBECTX}"       | tee -a ./.testenv >&2
		echo "KNIT_TEST_NAMESPACE=${KNIT_TEST_NAMESPACE}"   | tee -a ./.testenv >&2
		echo "---------" >&2
		echo "(these configs are written in \"$(pwd)/.testenv\" .)" >&2
		;;
	uninstall)
		up ${2}
		${HELM} ${HELMOPTS} uninstall --namespace ${KNIT_TEST_NAMESPACE} --wait knit-test || :
		${HELM} ${HELMOPTS} uninstall --namespace ${KNIT_TEST_NAMESPACE} --wait knit-schema-upgrader || :
		${HELM} ${HELMOPTS} uninstall --namespace ${KNIT_TEST_NAMESPACE} --wait knit-db-postgres || :
		${KUBECTL} ${KUBEOPTS} --namespace ${KNIT_TEST_NAMESPACE} delete pvc --all
		${KUBECTL} ${KUBEOPTS} delete pv --all
		${HELM} ${HELMOPTS} uninstall --namespace ${KNIT_TEST_NAMESPACE} --wait knit-storage-nfs
		;;
	test)
		shift || :
		up ${2}
		export KNIT_TEST_KUBECONFIG
		export KNIT_TEST_KUBECTX
		export KNIT_TEST_NAMESPACE

		echo "# (root)" >&2
		go test -p=1 $@ ./...
		for CMD in ./cmd/* ; do
			(
				cd ${CMD}
				echo "# ${CMD}" >&2
				go test -p=1 $@ ./...
			)
		done
		;;
	delete)
		${COLIMA} -p ${KNIT_TEST_COLIMA_PROFILE} delete
		;;
	stop)
		${COLIMA} -p ${KNIT_TEST_COLIMA_PROFILE} stop
		;;
	*)
		echo "unknown command '${1}'. should be one of: cni, install, upgrade, uninstall, test" >&2
		exit 1
		;;
esac
