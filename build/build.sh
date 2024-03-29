#! /bin/bash
set -e

HERE=${0%/*}
ROOT=$(cd ${HERE}/../; pwd)

HELM=${HELM:-helm}
DOCKER=${DOCKER:-docker}
REPOSITORY=${REPOSITORY:-}
BRANCH=${BRANCH:-main}

echo "$(date) - step [1/5]: go generate" >&2
go mod tidy
go generate ./...
for CMD in ./cmd/* ; do
	(
		cd ${CMD}
		go mod tidy
		go generate ./...
	)
done

CHART_DEST_LOCAL=${ROOT}/charts/local
CHART_DEST_TEST=${ROOT}/charts/test
CHART_DEST_RELEASE=${ROOT}/charts/release
CHART_DEST=${CHART_DEST_LOCAL}

BIN_DEST=${ROOT}/bin
BUILD_MODE=local
while [ -n "${1}" ] ; do
	ARG=${1}; shift
	case ${ARG} in
		-c|--clean)
			# deplicated. this is default and no longer needed.
			;;
		--test)
			BUILD_MODE=test
			CHART_DEST=${CHART_DEST_TEST}
			;;
		--release)
			BUILD_MODE=release
			CHART_DEST=${CHART_DEST_RELEASE}
			REPOSITORY=${REPOSITORY:-opst/knitfab}
			IMAGE_REGISTRY=${IMAGE_REGISTRY:-ghcr.io/${REPOSITORY}}
			CHART_REPOSITORY_ROOT=${CHART_REPOSITORY_ROOT:-"https://raw.githubusercontent.com/${REPOSITORY}/${BRANCH}/charts/release"}
			;;
		--debug)
			VARIANT=debug
			;;
		--ide)
			IDE_TYPE=${1}; shift
			;;
	esac
done

if [ "${VARIANT}" = "debug" ] && [ "${BUILD_MODE}" = "release" ] ; then
	echo "ERROR: --debug and --release are exclusive." >&2
	exit 1
fi

case "${IDE_TYPE}" in
	vscode)
		echo "generating configuration for IDE ${IDE_TYPE} ..."
		ROOT=${ROOT} ${HERE}/lib/ideconfig-${IDE_TYPE}.sh
		exit
	;;
	"") ;;
	*)
		echo "unknown IDE: ${IDE_TYPE}" >&2
		exit 1
	;;
esac

# step2. dump docker images
echo "$(date) - step [2/5]: build images" >&2

function detect_diff() {
	cd ${ROOT}
	(
		cat ./.dockerignore
		echo "!build"
	) \
	| grep -Ee '^!' \
	| while read LINE; do
		if ! git diff --quiet -- ${LINE/!/} ; then
			echo "-localdiff-"$(date +%Y%m%d%H%M%S)
			break
		fi
	done
}

if ! [ -n "${APP_VERSION}" ] ; then
	APP_VERSION=$(head -n 1 ${ROOT}/VERSION)
fi
if ! [ -n "${CHART_VERSION}" ] ; then
	CHART_VERSION=$(head -n 1 ${ROOT}/VERSION)
fi

HASH=$(git rev-parse --short HEAD)
LOCALDIFF=$(detect_diff)

if ! mkdir -p ${BIN_DEST} ; then
	echo "failed to create --dest directory: ${BIN_DEST}" >&2
	exit 1
fi
if ! mkdir -p ${CHART_DEST} ; then
	echo "failed to create directory: ${CHART_DEST}" >&2
	exit 1
fi

case ${BIN_DEST} in
	/|"")
		echo "ERROR: destination directory is root!" >&2
		exit 1
	;;
	*) ;;
esac

case ${CHART_DEST} in
	/|"")
		echo "ERROR: destination directory is root!" >&2
		exit 1
	;;
	*) ;;
esac

case ${BUILD_MODE} in
	release)
		if [ -n "${LOCALDIFF}" ] ; then
			echo "ERROR: local changes detected. cannot release." >&2
			exit 1
		fi
		APP_VERSION="${APP_VERSION}-${HASH}"
		;;
	test)
		APP_VERSION=TEST
		CHART_VERSION=v0.0.0-test
		;;
	local)
		APP_VERSION="${APP_VERSION}-${HASH}${LOCALDIFF}"
		TARGET_SUFFIX=
		case ${VARIANT} in
			debug)
				TARGET_SUFFIX="-debug"
				;;
			*)
				;;
		esac
		APP_VERSION="${APP_VERSION}${TARGET_SUFFIX}"
		echo ${APP_VERSION} > ${CHART_DEST_LOCAL}/VERSION
		echo ${CHART_VERSION} > ${CHART_DEST_LOCAL}/CHART_VERSION
		;;
esac

export APP_VERSION
export CHART_VERSION

echo "clear output directory ${BIN_DEST} ..." >&2
rm -rf ${BIN_DEST}/* >&2
echo "clear output directory ${CHART_DEST} ..." >&2
rm -rf ${CHART_DEST}/${CHART_VERSION} >&2

# step1. copy resources to dest
echo "$(date) - step [3/5]: copy assets to dest ${CHART_DEST}" >&2

for CHART in knit-app knit-certs knit-db-postgres knit-image-registry knit-storage-nfs ; do
	mkdir -p ${CHART_DEST}/${CHART_VERSION}/${CHART}
	cp -r ${ROOT}/charts/src/${CHART}/* ${CHART_DEST}/${CHART_VERSION}/${CHART}
	cat ${ROOT}/charts/src/${CHART}/Chart.yaml | envsubst > ${CHART_DEST}/${CHART_VERSION}/${CHART}/Chart.yaml
done

if [ -n "${KNIT_RELEASE_BUILD}" ] ; then
	echo "imageRepository: ${IMAGE_REGISTRY}" >> ${CHART_DEST}/${CHART_VERSION}/knit-app/values.yaml
fi

case ${VARIANT} in
	debug)
		echo "[--debug] put debugger-extras into charts/knit-app"
		cp -r ${HERE}/debugger-extras/* ${CHART_DEST}/${CHART_VERSION}/knit-app/templates
		;;
	*);;
esac

${HERE}/lib/build-images.sh knit-builder  # make layer-cache which has go modules/tools
${HERE}/lib/build-images.sh {knitd,knitd-backend,knit-vex,knit-empty,knit-nurse,knit-dataagt,knit-loops}${TARGET_SUFFIX}

case ${BUILD_MODE} in
	release)
		if [ -z "${IMAGE_REGISTRY}" ] ; then
			echo "FATAL: --release requires non-empty $\{IMAGE_REGISTRY\}" >&2
		fi
		for IMAGE in knitd knitd-backend knit-vex knit-empty knit-nurse knit-dataagt knit-loops ; do
			${DOCKER} tag ${IMAGE}:${APP_VERSION} ${IMAGE_REGISTRY}/${IMAGE}:${APP_VERSION}
		done
	;;
	local)
		function dumpimage() {
			mkdir -p ${CHART_DEST}/images
			echo -e "$1\t$1:${APP_VERSION}" >> ${CHART_DEST}/images/index
		}
		mkdir -p ${CHART_DEST}/images
		: > ${CHART_DEST}/images/index
		dumpimage knitd
		dumpimage knitd-backend
		dumpimage knit-vex
		dumpimage knit-empty
		dumpimage knit-nurse
		dumpimage knit-loops
		dumpimage knit-dataagt
	;;
esac

echo "$(date) - step [4/5]: build cli 'knit' @ ${BIN_DEST}" >&2
go mod download
LDFLAGS=
case ${BUILD_MODE} in
	release)
	LDFLAGS="-s"
	;;
	*)
	;;
esac
(
	cd ${ROOT}/cmd/knit
	go build ${LDFLAGS:+-ldflags} ${LDFLAGS} -o ${BIN_DEST}/knit .
)

for GOOS in linux darwin windows ; do
	for GOARCH in amd64 arm64 ; do
		echo "build cli 'knit-${GOOS}-${GOARCH}' @ ${BIN_DEST}" >&2
		EXT=
		if [ "${GOOS}" = "windows" ] ; then
			EXT=".exe"
		fi
		(
			cd ${ROOT}/cmd/knit
			GOOS=${GOOS} GOARCH=${GOARCH} go build -o ${BIN_DEST}/knit-${GOOS}-${GOARCH}${EXT} .
		)
	done
done

if [ "${BUILD_MODE}" != "release" ] ; then
	echo "$(date) - step [5/5]: skip." >&2
else
	echo "$(date) - step [5/5]: package helm charts" >&2
	for CHART in knit-app knit-certs knit-db-postgres knit-image-registry knit-storage-nfs ; do
		(
			cd ${CHART_DEST}/${CHART_VERSION};
			${HELM} package -u ${CHART};
		)
	done
	(
		cd ${CHART_DEST};
		${HELM} repo index --merge ./index.yaml --url "${CHART_REPOSITORY_ROOT}" .;
	)

	cat <<EOF >&2

**** THIS IS  RELEASE BUILD ****

You may need to release knitfab.

* To Publish Images
  1. "docker login" to ${IMAGE_REGISTRY%%/*}
    for more detail: https://docs.github.com/packages/working-with-a-github-packages-registry/working-with-the-container-registry#personal-access-token-classic
  2. "docker push" images:

EOF
	for IMAGE in knitd knitd-backend knit-vex knit-empty knit-nurse knit-dataagt knit-loops ; do
		echo "     docker push ${IMAGE_REGISTRY}/${IMAGE}:${APP_VERSION};" >&2
	done

	cat <<EOF >&2

* To Publish Helm Charts
  1. git add ${CHART_DEST}; git commit
  2. create a pull request & merge it

EOF
fi

echo "$(date) - build done!" >&2
