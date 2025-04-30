#! /bin/bash
set -e

HERE=$(cd ${0%/*}; pwd)

## start minikube cluster named "knitfab-dev-cluster" with 3 nodes, if needed
# (check if knirtfab-dev-cluster is already running)

PROFILE_MISSING=
if ! ((minikube profile list 2>/dev/null || : ) | grep -q "knitfab-dev-cluster") ; then
  PROFILE_MISSING=1
fi

if ! minikube status -p knitfab-dev-cluster | grep -q "Running" ; then
  ## check current k8s context
  BASE_KUBECONTEXT=$(kubectl config current-context 2> /dev/null || echo "")
  if [ -n "${BASE_KUBECONTEXT}" ] ; then
    trap "echo reset k8s context...; kubectl config use-context ${BASE_KUBECONTEXT}" EXIT
  fi
  minikube start -p knitfab-dev-cluster --nodes 1 --driver=qemu --container-runtime=containerd --memory=6g --cpus=2
  echo $(kubectl config current-context) > "${HERE}/.dev-cluster-kube-context"
fi

export KUBECONTEXT=$(cat "${HERE}/.dev-cluster-kube-context")

# install nfs-service
kubectl --context "${KUBECONTEXT}" apply -f "${HERE}/lib/nfs-service.kube.yaml"

# install image registry
if [ -n "${PROFILE_MISSING}" ] ; then
  rm -rf "${HERE}/docker-certs/meta"
fi
mkdir -p "${HERE}/docker-certs/meta"
export TLSCERT="${HERE}/docker-certs/meta/server.crt"
export TLSKEY="${HERE}/docker-certs/meta/server.key"
export TLSCACERT="${HERE}/docker-certs/meta/ca.crt"
export TLSCAKEY="${HERE}/docker-certs/meta/ca.key"

# detect IP addr of this machine
if [ -z "${IP}" ] ; then
  if which ip > /dev/null 2>&1 ; then
    IP=$(ip -4 addr show | grep -E 'inet (10|192|172)' | awk '{print $2}' | cut -d/ -f1)
  elif which ifconfig > /dev/null 2>&1 ; then
    IP=$(ifconfig | grep -E 'inet (10|192|172)' | awk '{print $2}')
  else 
    echo "Cannot find IP address of this machine. Please set IP manually."
    exit 1
  fi
fi

if [ -f ${TLSCERT} ] && [ -f ${TLSKEY} ] ; then
	echo "TLS certificate and key already exist"
else
  IPS=
  for I in ${IP} ; do
    IPS="${IPS} --ip ${I}"
  done
	"${HERE}/lib/new-tlscerts.sh" --dest "${HERE}/docker-certs/meta" --node-ips ${IPS} --name localhost
fi

NODE_IP=$(minikube -p knitfab-dev-cluster ip)
CERTS_DIR_MINIKUBE="${HOME}/.docker/certs.d/${NODE_IP}:30005"
echo "Importing TLS CA certificate into ${CERTS_DIR_MINIKUBE}" >&2
mkdir -p "${CERTS_DIR_MINIKUBE}"
cp ${TLSCACERT} ${CERTS_DIR_MINIKUBE}/ca.crt
echo  "This directory is created by knitfab-dev-cluser at $(date)" > "${CERTS_DIR_MINIKUBE}/knitfab-dev-cluster"

for I in ${IP} ; do
  if [ "${I}" != "${NODE_IP}" ] ; then
    D="${HOME}/.docker/certs.d/${I}:30005"
    CERTS_DIR_LOCAL="${CERTS_DIR_LOCAL:+,}${D}"
    echo "Importing TLS CA certificate into ${D}" >&2
    mkdir -p "${D}"
    cp ${TLSCACERT} ${D}/ca.crt
    echo  "This directory is created by knitfab-dev-cluser at $(date)" > "${D}/knitfab-dev-cluster"
  fi
done

"${HERE}/lib/install-registry.sh"

if [ -d "${HERE}/.up.d" ] ; then
  for HOOK in "${HERE}/.up.d/"*.sh ; do
    if [ -x "${HOOK}" ] ; then
      echo "* Running Hook: ${HOOK}" >&2
      "${HOOK}"
    fi
  done
fi

cat <<EOF >&2

# Minikube cluster knitfab-dev-cluster is up and running.

You can access the cluster using the following command:

    kubectl --context ${KUBECONTEXT} get ...

# TLS certificate and key for the image registry are located in:

    \{${CERTS_DIR_LOCAL}\}/ca.crt and
    ${CERTS_DIR_MINIKUBE}/ca.crt

(You may need to restart your dockerd to pick up the new certificate.)

# NFS Service is running in the cluster.

AFTER you run \`dev-cluster/install-knit.sh --prepare\`, you need to configure to use it.
Update your dev-cluster/knitfab-install-settings/values/knit-straoge-nfs.yaml like this:

    nfs:
        external: true
        server: "nfs-server.knitfab-dev-cluster-infra"

EOF
