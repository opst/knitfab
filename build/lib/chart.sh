#! /bin/bash
set -e

HERE=$(cd ${0%/*}; pwd)
ROOT=$(cd ${HERE}/../../; pwd)
DEST=${DEST:-${ROOT}/charts}

HELM=${HELM:-helm}

CHART_REGISTRY=${CHART_REGISTRY:-}   # e.g. http://raw.githubusercontent.com
REPOSITORY_PATH=${REPOSITORY_PATH:-} # e.g. opst/knitfab
BRANCH=${BRANCH:-main}
if [ -n "${CHART_REGISTRY}" ] ; then
	CHART_REPOSITORY="${CHART_REGISTRY}"  # https://raw.githubusercontent.com/opst/knitfab
	if [ -n "${REPOSITORY_PATH}" ] ; then
		CHART_REPOSITORY="${CHART_REPOSITORY}/${REPOSITORY_PATH}"
		if [ -n "${BRANCH}" ] ; then
			CHART_REPOSITORY="${CHART_REPOSITORY}/${BRANCH}"
		fi
	fi

elif [ "release" = "${BUILD_MODE}" ] ; then
	echo "CHART_REGISTRY and REPOSITORY_PATH is not specified!" >&2
	exit 1
fi

if [ -z "${APP_VERSION}" ] ; then
	echo "knit app version is not specified!" >&2
	exit 1
fi

if [ -z "${CHART_VERSION}" ] ; then
	echo "knit chart version is not specified!" >&2
	exit 1
fi

case "${BUILD_MODE}" in
	release|local|test)
		CHART_ROOT="${DEST}/${BUILD_MODE}"
		;;
	*)
		echo "unknown BUILD_MODE: ${BUILD_MODE}" >&2
		exit 1
		;;
esac

CHART_DEST="${CHART_ROOT}/${CHART_VERSION}"
rm -rf ${CHART_DEST}
mkdir -p ${CHART_DEST}

echo "building charts @ ${CHART_DEST}" >&2

APP_VERSION=${APP_VERSION}
if [ "release" != ${BUILD_MODE} ] ; then
	APP_VERSION=${APP_VERSION}-${BUILD_MODE}
fi
if [ -n "${DEBUG}" ] ; then
	APP_VERSION=${APP_VERSION}-debug
fi
export APP_VERSION
for CHART in knit-app knit-certs knit-db-postgres knit-image-registry knit-storage-nfs ; do
	mkdir -p ${CHART_DEST}/${CHART}
	cp -r ${ROOT}/charts/src/${CHART}/* ${CHART_DEST}/${CHART}

	cat ${ROOT}/charts/src/${CHART}/Chart.yaml | envsubst > ${CHART_DEST}/${CHART}/Chart.yaml
done

if [ -n "${DEBUG}" ] ; then
	echo "debugger-extras into knit-app"
	cp -r ${HERE}/../debugger-extras/* ${CHART_DEST}/knit-app/templates
fi

if [ -n "${CHART_REPOSITORY}" ] ; then
	for CHART in knit-app knit-certs knit-db-postgres knit-image-registry knit-storage-nfs ; do
		(
			cd ${CHART_DEST};
			${HELM} package -u ${CHART};
		)
	done
	(
		cd ${CHART_ROOT};
		${HELM} repo index --merge ./index.yaml --url "${CHART_REPOSITORY}/charts/${BUILD_MODE}" .;
	)
fi
