#! /bin/sh
set -e

HERE=${0%/*}
KUBECTL=${KUBECTL:-kubectl}
JQ=${JQ:-jq}
export KUBECONFIG=${KUBECONFIG:-${HERE}/.sync/kubeconfig/kubeconfig}
NAMESPACE=${NAMESPACE:-registry}

while [ -n "${1}" ] ; do
  ARG=${1}; shift
  case ${ARG} in
    --uninstall)
      UNINSTALL=true
      ;;
    --reset-cert)
      RESET_CERT=true
      ;;
    *)
  esac
done

if [ -n "${RESET_CERT}" ] ; then
  ${KUBECTL} delete secret registry-tls -n ${NAMESPACE} || :
  rm -rf ${HERE}/docker-certs/meta
fi

if [ -n "${UNINSTALL}" ] ; then
  ${KUBECTL} delete -n ${NAMESPACE} deployment image-registry || :
  ${KUBECTL} delete -n ${NAMESPACE} service image || :
  ${KUBECTL} delete -n ${NAMESPACE} pvc registry-backend || :
  ${KUBECTL} delete pv image-registry-backend || :
  ${KUBECTL} delete namespace ${NAMESPACE} || :
  exit
fi

if ${KUBECTL} get namespace ${NAMESPACE} > /dev/null 2>&1 ; then
	echo "Namespace ${NAMESPACE} already exists"
else
	${KUBECTL} apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: "${NAMESPACE}"
EOF
fi

mkdir -p ${HERE}/docker-certs/meta

TLSCERT=${HERE}/docker-certs/meta/server.crt
TLSKEY=${HERE}/docker-certs/meta/server.key
TLSCACERT=${HERE}/docker-certs/meta/ca.crt
TLSCAKEY=${HERE}/docker-certs/meta/ca.key

if [ -f ${TLSCERT} ] && [ -f ${TLSKEY} ] ; then
	echo "TLS certificate and key already exist"
else
	${HERE}/new-tlscerts.sh --dest ${HERE}/docker-certs/meta --name meta.registry --node-ips
fi

if ${KUBECTL} get secret registry-tls -n ${NAMESPACE} > /dev/null 2>&1 ; then
	echo "Secret registry-tls already exists"
else
	${KUBECTL} create secret tls registry-tls -n ${NAMESPACE} --cert=${TLSCERT} --key=${TLSKEY}

  NODE_IP=$(${KUBECTL} get node -o json knit-gateway | ${JQ} -r '.status.addresses[] | select(.type == "InternalIP") | .address')
  CERTS_DIR="${HOME}/.docker/certs.d/${NODE_IP}:30005"
  echo "Importing TLS CA certificate into ${CERTS_DIR} (this may effect when you restart colima)" >&2

  mkdir -p "${CERTS_DIR}"
  cp ${TLSCACERT} ${CERTS_DIR}/ca.crt
fi

if ${KUBECTL} get sc local-storage > /dev/null 2>&1 ; then
  echo "StorageClass \"local-storage\" already exists"
else
  ${KUBECTL} apply -f - <<EOF
# https://kubernetes.io/docs/concepts/storage/storage-classes/#local
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-storage
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
EOF
fi

if ${KUBECTL} get service -n ${NAMESPACE} image > /dev/null 2>&1 ; then
	echo "Image Registry Service \"image\" already exists"
else
	${KUBECTL} apply -n ${NAMESPACE} -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: image
spec:
  ports:
    - name: https
      nodePort: 30005
      port: 5000
  selector:
    app: registry
  type: NodePort

---

apiVersion: v1
kind: PersistentVolume
metadata:
  name: image-registry-backend
spec:
  capacity:
    storage: 5Gi
  volumeMode: Filesystem
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: local-storage
  local:
    path: /var/knitfab/external-image-registry
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
            - knit-gateway
  claimRef:
    namespace: ${NAMESPACE}
    name: registry-backend

---

apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: registry-backend
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: local-storage
  volumeName: image-registry-backend

---

apiVersion: apps/v1
kind: Deployment
metadata:
  name: image-registry
spec:
  replicas: 1
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      containers:
        - name: registry
          image: registry:2
          ports:
            - containerPort: 5000
              name: https
          env:
            - name: REGISTRY_HTTP_ADDR
              value: :5000
            - name: REGISTRY_HTTP_TLS_CERTIFICATE
              value: /certs/tls.crt
            - name: REGISTRY_HTTP_TLS_KEY
              value: /certs/tls.key
          volumeMounts:
            - name: registry-tls
              mountPath: /certs
      volumes:
        - name: registry-tls
          secret:
            secretName: registry-tls
        - name: backend
          persistentVolumeClaim:
            claimName: registry-backend
EOF
fi


echo "Image Registry Service \"image\" is ready" >&2

