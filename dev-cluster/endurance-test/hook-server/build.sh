#! /bin/bash
set -e

HERE=$(cd ${0%/*}; pwd)

(
	cd ${HERE}/../../../cmd/knit
	GOOS=linux go build -o ${HERE}/knit .
)

VERSION=v1.0
docker build -t knitfab-endurance-hook:${VERSION} "${HERE}"

NODE_IP=$(minikube -p $(cat ${HERE}/../../.dev-cluster-kube-context) ip)
docker tag knitfab-endurance-hook:${VERSION} ${NODE_IP}:30503/knitfab-endurance-hook:${VERSION}
docker push ${NODE_IP}:30503/knitfab-endurance-hook:${VERSION}
