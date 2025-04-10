#! /bin/bash
set -e

HERE=$(cd ${0%/*}; pwd)

docker build -t knitfab-endurance-task:v1.0 "${HERE}"

NODE_IP=$(minikube -p $(cat ${HERE}/../../.dev-cluster-kube-context) ip)
docker tag knitfab-endurance-task:v1.0 ${NODE_IP}:30503/knitfab-endurance-task:v1.0
docker push ${NODE_IP}:30503/knitfab-endurance-task:v1.0
