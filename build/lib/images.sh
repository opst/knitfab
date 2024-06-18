#! /bin/bash
set -e

ROOT=$(cd ${0%/*}/../../; pwd)
DEST=${DEST:-${ROOT}/bin/images}
rm -rf ${DEST}
mkdir -p ${DEST}

DOCKER=${DOCKER:-docker}

if [ -z "${APP_VERSION}" ] ; then
	echo "knit app version is not specified!" >&2
	exit 1
fi

IMAGE_REGISTRY=${IMAGE_REGISTRY:-}  # e.g. ghcr.io
export REPOSITORY=${REPOSITORY:-}          # e.g. opst/knitfab
if [ -n "${IMAGE_REGISTRY}" ] && [ -n "${REPOSITORY}" ] ; then
	IMAGE_REPOSITORY="${IMAGE_REGISTRY}/${REPOSITORY}"  # ghcr.io/opst/knitfab
fi

COMPONENT="knitd knitd-backend knit-vex knit-empty knit-nurse knit-dataagt knit-loops knit-schema-upgrader"
# build images...
for A in ${ARCH} ; do
	PLATFORM=
	TAG=${APP_VERSION}
	case ${A} in
		amd64|arm64)
			PLATFORM="linux/${A}"
			TAG="${APP_VERSION}-${A}"
			;;
		local)
			TAG="${APP_VERSION}-${A}"
			;;
		test|"")
			;;
		*)
			echo "unknown ARCH: ${A}" >&2
			exit 1
		;;
	esac


	TARGET=
	for COMP in ${COMPONENT} ; do
		TARGET="${TARGET} ${COMP}${SUFFIX}"
	done

	echo "building images for ${A}..." >&2
	BUILDX_SET=
	for COMP in ${COMPONENT} ; do
		BUILDX_SET="${BUILDX_SET} --set ${COMP}.platform=${PLATFORM}"
	done
	TAG=${TAG} PLATFORM=${PLATFORM} ${DOCKER} buildx bake --load \
		-f ${ROOT}/build/docker-compose.yml ${BUILDX_SET} \
		${TARGET}

	for COMP in ${COMPONENT} ; do
		echo "${COMP}	${COMP}:${TAG}"
	done
done

# generate image publish script...

echo "generating publish script @ ${DEST}/${BUILD_MODE}/publish.sh" >&2
mkdir -p ${DEST}/${BUILD_MODE}
(
cat <<EOF
#!/bin/bash
set -e

# publish images to ${IMAGE_REGISTRY}/${REPOSITORY} by default.
#
# This script is generated by build/lib/images.sh

IMAGE_REGISTRY="\${IMAGE_REGISTRY:-${IMAGE_REGISTRY}}"
REPOSITORY="\${REPOSITORY:-${REPOSITORY}}"

if [ -z "\${IMAGE_REGISTRY}" ] ; then
	echo "envvar IMAGE_REGISTRY is not set!" >&2
	exit 1
fi
if [ -z "\${REPOSITORY}" ] ; then
	echo "envvar REPOSITORY is not set!" >&2
	exit 1
fi
IMAGE_REPOSITORY="\${IMAGE_REGISTRY}/\${REPOSITORY}"

EOF

INSECURE=
if [ "${BUILD_MODE}" != "release" ]; then
	INSECURE="--insecure"
fi

for COMP in ${COMPONENT} ; do
	echo "echo \"publishing ${COMP}...\" >&2"
	VARIANTS=
	for A in ${ARCH} ; do
		VAR="${COMP}:${APP_VERSION}-${A}"
		VARIANTS="${VARIANTS} \${IMAGE_REPOSITORY}/${VAR}"
		echo "${DOCKER} tag \"${VAR}\" \"\${IMAGE_REPOSITORY}/${VAR}\""
		echo "while ! ${DOCKER} push \"\${IMAGE_REPOSITORY}/${VAR}\"; do echo 'retry...'; sleep 1; done"
		echo ""
	done

	echo "while ! ${DOCKER} manifest create ${INSECURE} --amend \"\${IMAGE_REPOSITORY}/${COMP}:${APP_VERSION}\" ${VARIANTS}; do echo 'retry...'; sleep 1; done"
	echo "while ! ${DOCKER} manifest push ${INSECURE} --purge \"\${IMAGE_REPOSITORY}/${COMP}:${APP_VERSION}\"; do echo 'retry...'; sleep 1; done"
	echo ""
done
) > ${DEST}/${BUILD_MODE}/publish.sh
