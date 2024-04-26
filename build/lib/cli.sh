#! /bin/bash
set -e
ROOT=$(cd ${0%/*}/../../; pwd)
DEST=${DEST:-${ROOT}/bin}
mkdir -p ${DEST}

case ${BUILD_MODE} in
	release)
		LDFLAGS="-s"
	;;
	local|test)
		# local build.
		EXT=
		if [ "windows" = $(go env GOOS) ] ; then
			EXT=".exe"
		fi
		echo "build cli 'knit${EXT}' @ ${DEST}" >&2
		go build ${LDFLAGS:+-ldflags} ${LDFLAGS} -o ${DEST}/knit${EXT} .
		exit
	;;
	*)
		echo "unknown BUILD_MODE: ${BUILD_MODE}" >&2
		exit 1
	;;
esac

# cross compile to make release builds

CLI_DEST=${DEST}/clis
rm -rf ${CLI_DEST}
mkdir -p ${CLI_DEST}
for GOOS in ${OS} ; do
	for GOARCH in ${ARCH} ; do
		echo "build cli 'knit-${GOOS}-${GOARCH}' @ ${CLI_DEST}" >&2
		EXT=
		if [ "${GOOS}" = "windows" ] ; then
			EXT=".exe"
		fi
		(
			cd ${ROOT}/cmd/knit
			GOOS=${GOOS} GOARCH=${GOARCH} go build ${LDFLAGS:+-ldflags} ${LDFLAGS} -o ${CLI_DEST}/knit-${GOOS}-${GOARCH}${EXT} .
		)
	done
done
