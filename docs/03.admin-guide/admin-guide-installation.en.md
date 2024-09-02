Knitfab Administration Guide: 1.Installation <!-- omit in toc -->

Table of Contents
- [1. Introduction](#1-introduction)
  - [1.1. Translations/他言語版](#11-translations他言語版)
  - [1.2. Important Notes](#12-important-notes)
- [2. Preparation for Installing Knitfab](#2-preparation-for-installing-knitfab)
  - [2.1. Kubernetes](#21-kubernetes)
  - [2.2. NFS Server](#22-nfs-server)
- [3. Installing Knitfab](#3-installing-knitfab)
  - [3.1. What is installed](#31-what-is-installed)
  - [3.2. Prerequisites](#32-prerequisites)
  - [3.3. Installation steps](#33-installation-steps)
- [4. Uninstall Knitfab](#4-uninstall-knitfab)
- [5. Helm configuration for Knitfab](#5-helm-configuration-for-knitfab)


# 1. Introduction

This document is for individuals who are responsible for operating and managing Knitfab.

Topics covered include:

- How to install Knitfab
- Operational considerations for Knitfab
- Kubernetes resources that make up Knitfab

These topics may go beyond the scope of interest for users who do not manage or operate Knitfab.

The file is divided into Part 1 and Part 2. Part 1 mainly explains the
installation procedure, and Part 2 mainly explains the operation and management
of Knitfab after installation.

## 1.1. Translations/他言語版

- Japanese:
  - [./admin-guide-installation.ja.md](./admin-guide-installation.ja.md)
  - [./admin-guide-deep-dive.ja.md](./admin-guide-deep-dive.ja.md)

## 1.2. Important Notes

> [!Caution]
>
> **Do not expose Knitfab to public networks.**
>
> Currently, Knitfab and the image registry within the cluster have no
> authentication or authorization mechanisms.
>
> Exposing it to the public internet poses the following risks:
>
> - Malicious containers may be executed
> - Malicious container images may be distributed
>
> The former not only consumes computational resources but also exposes you to
> further threats by exploiting unknown vulnerabilities in Kubernetes. The
> latter can also be used as a stepping stone for other threats.
>
> **Once again, do not expose Knitfab to the public internet.**
>

# 2. Preparation for Installing Knitfab

Before starting the installation process of Knitfab, you need to prepare an environment that meets the following requirements.

- Kubernetes Cluster:
  - **Kubernetes** (also called as K8s) is an open-source container
    orchestration system for deploying, scaling, and managing containerized
    applications.
  - Knitfab runs on a Kubernetes cluster.
  - It can be a multi-node cluster or a single-node cluster.
  - A K8s cluster consisting of Nodes equipped with x86_64 CPUs.
  - The cluster must be able to access the internet.
- NFS:
  - **NFS (Network File System)** is a distributed file system and its protocol mainly used in UNIX.
  - To persist RDB, cluster-internal image registry, and Knitfab data, NFS is used.

In particular, NFS will serve as the destination for users to accumulate data. It is recommended to allocate sufficient storage capacity.

## 2.1. Kubernetes

Please refer to the following official documentation for instructions on how to set up Kubernetes.

- https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/create-cluster-kubeadm/
- https://kubernetes.io/docs/setup/production-environment/container-runtimes/
- https://kubernetes.io/docs/tasks/administer-cluster/kubeadm/configure-cgroup-driver/

Note that the Knitfab development team has tested the operation on a Kubernetes cluster built with the following conditions:

- Kubernetes 1.29.2
- Container runtime: containerd
- Cgroup: systemd

### 2.1.1. Install CNI

To enable the network functionality of Kubernetes, you need to install some Container Network Interface (CNI).

The Knitfab development team has tested the operation with [calico](https://docs.tigera.io/calico/latest/about).

### 2.1.2. Enable GPU

To enable the use of GPUs from containers on Kubernetes, you need to configure the nodes accordingly.

Please refer to the official documentation for instructions on how to set this up.

- https://kubernetes.io/ja/docs/tasks/manage-gpus/scheduling-gpus/

### 2.1.3. Single Node Cluster

If you are operating a Kubernetes cluster with only a single node (control plane node), you need to remove the taint specified on that node.
Otherwise, the components of Knitfab will not be able to start on any node.

For more details, please refer to https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/create-cluster-kubeadm/#control-plane-node-isolation.

## 2.2. NFS Server

Knitfab adopts the storage driver [csi-driver-nfs](https://github.com/kubernetes-csi/csi-driver-nfs) as the default [storage class](https://kubernetes.io/docs/concepts/storage/storage-classes/). This is done to ensure access to Knitfab data regardless of the node on which the container is launched.

Knitfab assumes NFSv4.

Therefore, please set up NFS in a location on the network accessible from each node of the Kubernetes cluster.
It should be sufficient to use an NAS with NFS capabilities, but it is also possible to set up nfsd on an existing machine.

> For example, on Ubuntu,
>
> - Install the `nfs-kernel-server` package (`apt install nfs-kernel-server`), and
> - Configure the settings file in `/etc/exports`
>
> to set up an NFS server.

# 3. Installing Knitfab

## 3.1. What is installed

Following items are installed in your Kubernetes cluster.

|  | Corresponding Helm Chart |
|:------|:------------|
| Knitfab Application | knit-app, knit-schema-upgrader |
| Database | knit-db-postgres |
| In-cluster Image Registry | knit-image-registry |
| TLS Certifications | knit-certs |
| StorageClass | knit-storage-nfs |

[CSI "csi-driver-nfs"](https://github.com/kubernetes-csi/csi-driver-nfs/) is also installed in the same Namespace as Knitfab, since "knit-storage-nfs" depends on it.

## 3.2. Prerequisites

To install Knitfab, the following tools are required:

- [helm](https://helm.sh/)
- bash
- wget

In addition, internet access and a kubeconfig file with access to the target Kubernetes cluster are also required.

If you are planning to configure a single-node cluster, at least 4GB of memory is required.
Note that this requirement is only the minimum for Knitfab to start. Depending on the machine learning tasks being executed, more memory may be required.

### 3.2.1. (Optional) Prepare TLS Certificates

Knitfab Web API and the in-cluster Image Registry communicate over HTTPS, by default.
The installer script generates certificates for this purpose, but you can also specify specific certificates to use.

- If you have a CA certificate and its key, you can use them.
- Additionally, if you have a server certificate and its key, you can use them.

For example, if you have a requirement such as "I want to use a specific domain name for the nodes in the Kubernetes cluster," you will need a server certificate and a CA certificate signed by it (along with their keys).

If no certificates are provided, the installer will generate self-signed certificates and a server certificate signed by them. The server sertificate has SAN with IP Addresses of nodes of the Kubernetes cluster where Knitfab is installed in.

## 3.3. Installation steps

1. Obtain the installer.
2. Generate the installation settings file and adjust the parameters.
3. Execute the installation.
4. Distribute the handout to users and have them start using it.

### 3.3.1. Obtain the installer

The installer is located at https://github.com/opst/Knitfab/installer/installer.sh.

Download it to a suitable directory.

```
mkdir -p ~/Knitfab/install
cd ~/Knitfab/install
wget -O installer.sh https://raw.githubusercontent.com/opst/Knitfab/main/installer/installer.sh
chmod +x ./installer.sh
```

### 3.3.2. Generate the installation settings file and adjust the parameters

The following Command generates the installation settings for Knitfab in the `./knitfab_install_settings` directory:

```
./installer.sh --prepare --kubeconfig ${YOUR_KUBECONFIG}
```

> [!Note]
>
> If you want to use specific TLS certificates, execute the following command instead.
>
> ```
> TLSCACERT=path/to/ca.crt TLSCAKEY=path/to/ca.key TLSCERT=path/to/server.crt TLSKEY=path/to/server.key ./installler.sh --prepare
> ```
>
> If there is no specification for the server certificate, omit the environment variables `TLSCERT` and `TLSKEY` and do the following:
>
> ```
> TLSCACERT=path/to/ca.crt TLSCAKEY=path/to/ca.key ./installler.sh --prepare
> ```
>

> [!Note]
>
> **Advanced**
>
> By the step above, Knitfab Web API is exposed as an https endpoint.
>
> However, it might be inconvinient that Knitfab itself is https. For example, deploying a load balancer front of Knitfab Web API, and you want for the LB to terminate TLS.
>
> In a case like that, add a flag `--no-tls` to step 2.
>
> ```
> ./installer.sh --prepare --no-tls --kubeconfig ${YOUR_KUBECONFIG}
> ```
>
> By `--no-tls`, `./installer.sh` does not generate TLS certificates and related configurations, then Knitfab Web API is not to be https on installing.
>
> If you do so, the in-cluster Image Repository is not https, either.
>
> Your users should register it as "insecure registry" to dockerd. For more details, see following links:
>
> - https://docs.docker.com/reference/cli/dockerd/#insecure-registries
> - https://docs.docker.com/reference/cli/dockerd/#daemon-configuration-file
>

> [!Caution]
>
> **If you specify TLS certificates, those certificates and secret keys will be copied as part of the installation settings.**
>
> - `knitfab-install-settings/certs/*` (key pair; as file copies)
> - `knitfab-install-settings/values/knit-certs.yaml` (key pair; as base64-encoded text)
> - `knitfab-install-settings/knitprofile` (only cert; as base64-encoded text)
>
> Also, when key pair is generated, the key pair or certificaiton are stored as above.
>
> Especially, key pair has **secret key**. Handle with care.

#### 3.3.2.1. Configure to use NFS

**The default configuration generated by this command is set to "not persist Knitfab-managed information."**

To persist data using the prepared NFS, update the configuration.

The file to be updated is `Knitfab-install-settings/values/knit-storage-nfs.yaml`.
Update the following entries:

- `nfs.external`: Set the value to `true`.
- `nfs.server`: Comment in and specify the hostname (or IP) of the NFS server.

Additionally, update the following entries if necessary:

- `nfs.mountOptions`: Update if there are specific mount options for NFS.
- `nfs.share`: Specify the subdirectory you want to use for Knitfab.
    - The subdirectory needs to be created beforehand.

The configuration should look as follow:

```yaml
nfs:
  # # external: If true (External mode), use NFS server you own.
  # #  Otherwise(In-cluster mode), Knitfab employs in-cluster NFS server.
  external: true

  # # mountOptions: (optional) Mount options for the nfs server.
  # #  By default, "nfsvers=4.1,rsize=8192,wsize=8192,hard,nolock".
  mountOptions: "nfsvers=4.1,rsize=8192,wsize=8192,hard,nolock"

  # # # FOR EXTERNAL MODE # # #

  # # server: Hostname of the nfs server.
  # #  If external is true, this value is required.
  server: "nfs.example.com"  # update this to your NFS server host.

  # # share: (optional) Export root of the nfs server. default is "/".
  share: "/"

  # # # FOR IN-CLUSTER MODE # # #

  # # hostPath: (optional) Effective only when external is false.
  # # If set, the in-cluster NFS server will read/write files at this directory ON NODE.
  # #
  # # This is useful when you want to keep the data even after the NFS server is restarted.
  # hostPath: "/var/lib/Knitfab"

  # # node: (optional) Kubernetes node name where the in-cluster NFS server pod should be scheduled.
  # node: "nfs-server"
```

#### 3.3.2.2. Other installation parameters

For other files as well, you can modify the parameters as needed.

The following are particularly impactful for usage:

##### (1) Port No.

- `knitfab-install-settings/values/knit-app.yaml`'s `knitd.port`
- `knitfab-install-settings/values/knit-image-registry.yaml`'s `port`

The former is the listening port for the Knitfab API, and the latter is the listening port for the in-cluster image registry.

##### (2) TLD of the cluster
Also, if you have changed the TLD (Top-Level Domain) of the Kubernetes cluster during its setup from the default value (`cluster.local`), set the custom TLD in the following item.

- `clusterTLD` in `knitfab-install-settings/values/knit-app.yaml` (Comment in and modify)

##### (3) Knitfab extentions
There are configuration files to extend Knitfab's behavior.

- to register WebHooks, edit `knitfab-install-settings/values/hooks.yaml`
- to register Extra API, edit `knitfab-install-settings/valuesextra-api.yaml`

For more detail, see section "Extend Knitfab".

### 3.3.3. Step 3: Install

By executing the following command, the installation script will sequentially
install the components of Knitfab into the Kubernetes cluster. Please specify
the Kubernetes namespace name where the Knitfab application will be installed in
`${NAMESPACE}`. (Specify a new one here.) This will **take some time**.

```
./installer.sh --install --kubeconfig path/to/kubeconfig -n ${NAMESPACE} -s ./knitfab-install-settings
```
You can determine whether the installation was successful from the state of the
K8s deployment.

By using the `kubectl get deploy -A` command as shown below, check the 'READY'
value of the deployments in the namespace where Knitfab was installed. If all
the numerator and denominator values match, like 'N/N', then the installation
was successful.
```
$ kubectl get deploy -A
NAMESPACE          NAME                        READY   UP-TO-DATE   AVAILABLE   AGE
calico-apiserver   calico-apiserver            2/2     2            2           21d
calico-system      calico-kube-controllers     1/1     1            1           21d
calico-system      calico-typha                2/2     2            2           21d
kf-mycluster       csi-nfs-controller          1/1     1            1           19d
kf-mycluster       database-postgres           1/1     1            1           19d
kf-mycluster       finishing-leader            1/1     1            1           19d
kf-mycluster       garbage-collection-leader   1/1     1            1           19d
kf-mycluster       housekeeping-leader         1/1     1            1           19d
kf-mycluster       image-registry-registry     1/1     1            1           19d
kf-mycluster       initialize-leader           1/1     1            1           19d
kf-mycluster       knitd                       1/1     1            1           19d
kf-mycluster       knitd-backend               1/1     1            1           19d
kf-mycluster       projection-leader           1/1     1            1           19d
kf-mycluster       run-management-leader       1/1     1            1           19d
kube-system        coredns                     2/2     2            2           21d
tigera-operator    tigera-operator             1/1     1            1           21d

```
In the above example, Knitfab is installed in the namespace `kf-mycluster`. You
can see that the READY values of the deployments (NAME column) belonging to this
namespace are all '1/1'.

It may take some time to reach this state, so if the READY values are not 'N/N',
wait a bit and try again.

If it still doesn't work, refer to
[Troubleshooting](admin-guide-deep-dive.en.md#5-troubleshooting) for further
assistance.

If you don't want to display information for namespaces unrelated to Knitfab,
use the `-n` option as shown below.

```
$ kubectl get deploy -n kf-mycluster
```
Replace 'kf-mycluster' with the namespace name of your system.

### 3.3.4. Step 4: Distribute handouts to users

The connection information to the installed Knitfab is generated in the `knitfab-install-settings/handout` folder.

Distribute this folder to users who want to use Knitfab.

The usage instructions for this handout are described in the user guide.

#### 3.3.4.1. (Optional) Modify the handout

If you want to access Knitfab with a specific domain name (e.g., when a specified server certificate is configured), you need to modify the connection settings before distributing the handout to users.

The connection settings to the Knitfab API, called **knitprofil file**, can be found in `knitfab-install-settings/handout/knitprofile`. This file is a YAML file with the following structure:

```yaml
apiRoot: https://IP-ADDRESS:PORT/api
cert:
    ca: ...Certification....
```

The value of the `apiRoot` key indicates the endpoint of the Knitfab Web API.
By default, it should be set to the IP address of a appropriate node in the cluster.

If you want to access Knitfab with a specific domain name instead of an IP address, you need to modify this item.

For example, if you want to access Knitfab as `example.com:30803`, you can rewrite the host part of the `apiRoot` as follows:

```yaml
apiRoot: https://example.com:30803/api
cert:
    ca: ...Certification....
```

Also, you need to address the certificate for the **in-cluster Image Registry**.

You will find a directory named `knitfab-install-settings/handout/docker/certs.d/10.10.0.3:30503`.
This directory is also named after the IP address of a appropriate Kubernetes node concatenated with the port number using `:` as a separator.
Rename the part with this IP to the desired domain name for access.

# 4. Uninstall Knitfab

When you execute the installation, an uninstaller will be generated as `knitfab-install-settings/uninstall.sh`.

```
Knitfab-install-settings/uninstall.sh
```

Executing this command will uninstall the Knitfab application within the cluster.

Furthermore,

```
Knitfab-install-settings/uninstall.sh --hard
```

Executing this command will destroy all Knitfab-related resources, including the database and the in-cluster image registry.


# 5. Helm configuration for Knitfab

Knitfab is composed of several helm charts. This section explains the helm-based construction of Knitfab.

Administrators may need to uninstall, reinstall, or update parts of Knitfab. This section provides guidance on what to do in such cases, providing clarity on the necessary steps.

> [!Note]
>
> This section assumes that the reader has knowledge of helm.

Knitfab is composed of the following helm charts:

- Knitfab/knit-storage-nfs: Introduces the NFS driver and defines the StorageClass.
- Knitfab/knit-certs: Introduces certificates.
- Knitfab/knit-db-postgres: Defines the RDB.
- Knitfab/knit-image-registry: Defines the cluster's internal registry.
- Knitfab/knit-app: Defines other components of Knitfab not covered above.

The helm chart repository "Knitfab" is (by default) located at https://raw.githubusercontent.com/opst/Knitfab/main/charts/release.

By following the appropriate steps to install these charts, Knitfab can be installed.
In fact, the installer does exactly that.

In general, Knitfab is installed using the following steps:

```sh
NAMESPACE=${NAMESPACE}  # where Knitfab to be installed
CHART_VERSION=${CHART_VERSION:=v1.0.0}  # version of Knitfab to be installed
VALUES=./knit-install-settings/values

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    -f ${VALUES}/knit-storage-nfs.yaml \
    knit-storage-nfs Knitfab/knit-storage-nfs

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    -f ${VALUES}/knit-certs.yaml \
    knit-certs Knitfab/knit-certs

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    --set-json "storage=$(helm get values knit-storage-nfs -n ${NAMESPACE} -o json --all)" \
    -f ${VALUES}/knit-db-postgres.yaml \
    knit-db-postgres Knitfab/knit-db-postgres

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    --set-json "storage=$(helm get values knit-storage-nfs -n ${NAMESPACE} -o json --all)" \
    --set-json "certs=$(helm get values knit-certs -n ${NAMESPACE} -o json --all)" \
    -f ${VALUES}/knit-image-registry.yaml \
    knit-image-registry Knitfab/knit-image-registry

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    --set-json "storage=$(helm get values knit-storage-nfs -n ${NAMESPACE} -o json --all)" \
    --set-json "database=$(helm get values knit-db-postgres -n ${NAMESPACE} -o json --all)" \
    --set-json "imageRegistry=$(helm get values knit-image-registry -n ${NAMESPACE} -o json --all)" \
    --set-json "certs=$(helm get values knit-certs -n ${NAMESPACE} -o json --all)" \
    -f ${VALUES}/knit-app.yaml \
    knit-app Knitfab/knit-app
```

> In addition to the above operations, the installer provides additional options to make these behaviors more stable and generates uninstallers and handouts.

The pattern `--set-json "...=$(helm get values ...)"` that appears frequently in the middle is used to read installation parameters ([Helm Values](https://helm.sh/docs/chart_template_guide/values_files/)) from installed charts and ensure consistency between charts.

In addition, `./knitfab-install-settings/values/CHART_NAME.yaml` is incorporated as the Values for that chart.
Therefore, if you need to reinstall or update only a specific chart, you should follow this approach.

> [!Caution]
>
> Uninstalling the following charts will result in the loss of lineage and data in Knitfab. Please be cautious when uninstalling charts.
>
> - knitfab/knit-storage-nfs
> - knitfab/knit-db-postgres
> - knitfab/knit-image-registry
>
> knit-db-postgres and knit-image-registry also define PVCs, so uninstalling these charts will result in the loss of the previous database content and `docker push`ed images.
> As a result, the relationship between PVCs and Knitfab data, as well as the images referenced by Plans, will be lost, and the premise of Knitfab's lineage management will not be met.
>
> Additionally, knit-storage-nfs provides the functionality to store all other PVs on NFS. If this is lost, all Pods will no longer have access to PVs.

