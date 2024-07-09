#! /bin/bash
set -e

HERE=$(cd ${0%/*}; pwd)
ROOT=$(cd ${HERE}/../../; pwd)
DEST=${DEST:-${ROOT}/charts}

HELM=${HELM:-helm}

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

if [ -z "${APP_VERSION}" ] ; then
	echo "knit app version is not specified!" >&2
	exit 1
fi

export APP_VERSION
for CHART in knit-app knit-certs knit-db-postgres knit-image-registry knit-storage-nfs knit-schema-upgrader ; do
	mkdir -p "${CHART_DEST}/${CHART}"
	cp -r "${ROOT}/charts/src/${CHART}/"* "${CHART_DEST}/${CHART}"

	cat ${ROOT}/charts/src/${CHART}/Chart.yaml | envsubst > ${CHART_DEST}/${CHART}/Chart.yaml
done

if [ -n "${DEBUG}" ] ; then
	echo "debugger-extras into knit-app"
	cp -r ${HERE}/../debugger-extras/* ${CHART_DEST}/knit-app/templates
fi

for CHART in knit-app knit-certs knit-db-postgres knit-image-registry knit-storage-nfs knit-schema-upgrader ; do
	(
		cd ${CHART_DEST};
		${HELM} package -u ${CHART};
	)
done
(
	cd ${CHART_ROOT};
	${HELM} repo index --url ./ --merge ./index.yaml .;
)
