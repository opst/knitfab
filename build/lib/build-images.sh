#! /bin/bash
set -e
cd ${0%/*}
ROOT=$(cd ../../; pwd)

# detect dafault "docker-compose". Once it was "docker-compose", and now, it is "docker compose".
DEFAULT_DOCKER_COMPOSE="docker-compose"
case $(type -t ${DEFAULT_DOCKER_COMPOSE}) in
	alias|function)
		# when alias or function, rely on it.
		# user may setup it reasonably.
		;;
	file)
		if ! [ -x "$(type -p ${DEFAULT_DOCKER_COMPOSE})" ] ; then
			# if it is not executable, switch to docker.
			DEFAULT_DOCKER_COMPOSE="docker compose"
		fi
		;;
	*)
		# there are no docker-compose. fallback to docker.
		DEFAULT_DOCKER_COMPOSE="docker compose"
		;;
esac

# user can override docker-compose in one's own favor.
DOCKER_COMPOSE=${DOCKER_COMPOSE:-${DEFAULT_DOCKER_COMPOSE}}

DOCKER_BUILDKIT=1
export DOCKER_BUILDKIT

if [ -z "${APP_VERSION}" ] ; then
	echo "knit app version is not specified!" >&2
	exit 1
fi
export APP_VERSION

${DOCKER_COMPOSE} build "$@"
