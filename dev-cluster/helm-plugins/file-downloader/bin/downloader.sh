#! /bin/bash

CERT_FILE=${1}
KEY_FILE=${2}
CA_FILE=${3}
FULL_URL=${4}

FILEPATH=
case ${FULL_URL} in
	file://*)
		FILEPATH=${FULL_URL#file://}
		;;
	*)
		echo "unknown URL: ${FULL_URL}" >&2
		exit 1
		;;
esac

cat ${FILEPATH}
