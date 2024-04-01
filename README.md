Knitfab
=======

![Knitfab logo](./logo.png)

MLOps system & tool. Release AI/ML engineers from trivial routines.

- Automatic tracking of 'lineage'
- Dynamic workflow based on 'tags'
- Use of k8s and container to isolate ML tasks

Directory layout
===================

- `build/`: Scripts for building Knitfab
- `docs`: Documents.
- `cmd/`: Commands, say entry points of programmes.
    - `dataagt` : A single function web server -- read/write data into a directory.
        - Started as a k8s pod/service, used to create/read from Persistent Volumes of k8s.
    - `knit` : CLI of Knitfab API
    - `knit_nurse` : Runs as a sidecar of Knitfab worker to track logs.
    - `knitd` : The frontend API server of Knitfab, facing user requests (via Knitfab command).
    - `knitd_backend` : The internal server of Knitfab, using k8s API.
    - `volume_expander` : Runs as a k8s DaemonSet, watches k8s PVCs and resizes them.
- `charts/`: Helm charts that create test environments.
    - `src/`: The source files (meta-templates) of the charts.
    - `release/` : Released charts.
    - `local/` : Charts for local trials in dev-cluster (created when local build).
    - `test/` : Charts for automated tests (created when test).
- `dev-cluster/`: The provisioner of a local virtual k8s cluster.
- `installer/`:
    - `installer.sh`: A Web-based installer script.
- `internal/`: Internal Golang packages.
- `pkg/`: Golang packages.
- `go.{mod,sum}`
- `README.md` : This file.
- `LICENSE`: BSL-1.1.
- `testctl.sh` : A test environment provisioner and test runner.

Getting Started
================

Read [docs/01.getting-started](docs/01.getting-started).

For more detail, see [docs/02.user-guide](docs/02.user-guide).

How to Install
==============

Read [docs/03.admin-guide](docs/03.admin-guide).

Build Knitfab
==============

Our build script is `./build/build.sh` .
This script builds Knitfab into an installer bundle.

To build, just run:

```
./build/build.sh
```

### Usage

```
# build images and charts
./build/build.sh [--debug] [--release] [--test]

# generate debug configuration for IDE
./build/build.sh --ide vscode
```

### Build images and charts

```
./build/build.sh [--debug] [--release] [--test]

OPTIONS:

    --debug       do "debug build"
    --release     do "release build"
    --test        do build for automated test
```

The `--release` and `--test` options are mutually exclusive.
If neither `--release` nor `--test` are specified, it performs local build (for dev-cluster).

`./build/build.sh` requires commands below:

- bash
- docker and docker-compose (or docker compose) with buildkit
- base64
- git
- envsubst

### Debug build

When `./build/build.sh --debug`, it generates "debug mode" installer.

Debug mode installer has additional items & features.

- Images execute Knitfab comoponents with [`dlv exec`](https://github.com/go-delve/delve).
- K8s services routed to the `dlv` debugger server.

So, when you deploy Knitfab from "debug mode" installer,
the cluster exposes following ports to be attached with `dlv` and compatible IDE.

- knitd: `<any cluster node>:30990`
- knitd-backend : `<any cluster node>:30991`

You can get configurations for your IDE to attach knitd/knitd-backend with `./build/build.sh --ide <IDE_NAME>`.

Currently, we supports `--ide vscode` only.

If your preferred IDE is not supported, write your own config and share it with us.

### version tag

By default, container images are tagged as below:

```
${COMPONENT NAME}:${VERSION}-${GIT HASH}
```

- `COMPONENT NAME`: name of Knitfab component.
    - for example:  `knitd`, `knitd-backend`, `dataagt`, ...
- `VERSION` : the first line of `./VERSION` file.
- `GIT HASH` : short hash of the commit when you run `./build/build.sh`
    - It may be omitted when the env var `RELEASE_BUILD` is not empty.

If it is built as local build and your working copy has diff from `HEAD`,
image will be suffixed with `-localdiff-${TIMESTAMP}`.

The dev-cluster: A k8s cluster for developers
==================================

This repository contains provisioning scripts to deploy local Kubernetes cluster, based on **virtualbox**+**vagrant**+**ansible**.

You can use the cluster to try out or debug Knitfab.

### Prerequisites

- [vagrant](https://www.vagrantup.com/docs/installation)
- [virtualbox](https://www.virtualbox.org/) : A dependency of vagrant, to create virtual machiness.
- [poetry](https://python-poetry.org/) : Used to install ansible.
- [python](https://www.python.org/) (3.8+) : A dependency of Poetry.
- [docker, docker compose](https://docs.docker.com/) : Build and push image.
- (optional) [pyenv](https://github.com/pyenv/pyenv) : Used to manage your python installation.

Additionaly, `dev-cluster` uses [ansible](https://docs.ansible.com/ansible/latest/index.html), but it will be installed by Poetry.

### To start k8s cluster

To start, move to the `dev-cluster` directry, and run:

```
$ poetry install
$ poetry shell
```
(to install ansible)

and

```
$ vagrant up
```
(create vms & start provisioning with ansible)

It takes 10+ minutes at least, and can be over 30 minutes. Please be patient.
If you want to throw away them all, just do `vagrant destroy -f` and the VMs and k8s clusters will be destroyed.

### How is the dev-cluster provisioned?

The dev-cluster is a k8s cluster with the following nodes (VirtualBox VM):

- `knit-master` (default IP: `10.10.0.2`)
    - k8s master node.
- `knit-gateway` (default IP: `10.10.0.3`)
    - One of the k8s worker nodes.
    - It is also an NFS Server.
- `knit-nodes-${n}` (default IP: `10.10.0.4` or more)
    - k8s worker nodes.

During provisioning, it generates records of the cluster's configurations in the `.sync` directory:

- `.sync/cert/{ca,server}.{crt,key}`: A self-signed certificate and key pair, including the certificated IP address of the `knit-gateway` node.
- `.sync/kubeconfig/kubeconfig`: kubeconfig
- `.sync/kubeadm`: output of `kubeadm token`
- `.sync/knit/install_setting`: Knitfab installation setting (used by `./dev-cluster/install-knit.sh`)

### Install Knitfab into the dev-cluster

1. Copy `./dev-cluster/.sync/certs/ca.crt` to your `/etc/docker/certs.d/${VM-KNIT-GATEWAY-IP}:${IMAGE-REGISTRY-PORT}` (only when dev-cluster recreating)
    - By default, `${VM-KNIT-GATEWAY-IP}:${IMAGE-REGITRY-PORT}` is `10.10.0.3:30503`.
    - `${VM-KNIT-GATEWAY-IP}` should be the IP address of VirtualBox's VM instance "knit-gateway".
    - `${IMAGE-REGISTRY-PORT}` is defined in the chart value `./charts/local/v*/knit-image-registry/values.yaml` .
2. Run `./build/build.sh`
3. Then run `./dev-cluster/install-knit.sh`
    - It generates the `./configure/handout` directory, which contains the knitprofile file.
4. Start using Knitfab:
    - `./bin/knit init ./configure/handout/knitprofile`

> **Note**
>
> If you using colima (or docker-machine, minikube) as dockerd, you should put `ca.crt`
> on the (virtual) machine the dockerd process runs.
>
> For example, in the case of colima, `ca.crt` should place `/etc/docker/certs.d/...` *IN COLIMA*.
> You may need to `colima ssh` and copy the file.
>

#### `./dev-cluster/knitctl.sh`

`./dev-cluster/knitctl.sh` is a wrapper for `KUBECONFIG=... kubectl`.

So, you can just do:

```
./dev-cluster/knitctl.sh ...
```

instead of:

```
$ kubectl --kubeconfig ./dev-cluster/kubeconfig/kubeconfig ...
```

TEST ENVIRONMENT
-----------------

We provide a script, `./testctl.sh`, to create the test environment.

It uses **colima** to create a k8s cluster for the test.

This test environment is ISOLATED FROM the `dev-cluster`, because it is too large to create and remove frequently.
The test environment should be *lightweight* and capable of being created and removed frequently.

It is recommended to expand the memory of the colima instance to 8GB or more.
To do this, update your `~/.colima/_template/default`.

To get up a new environment, activate your colima, and run:

```
./testctl.sh install
```

To test, run

```
./testctl.sh test
```

> [!Note]
>
> Kubernetes in the colima VM and VirtualBox can have conflicts of networking.
>
> Sometimes, you may need to suspend the dev-cluster.

### Environment variables

`./testctl.sh` saves env-vars in the `.testenv` file.

- `KNIT_TEST_KUBECONFIG` : kubeconfig file to be used in test.
- `KNIT_TEST_KUBECTX`    : k8s context to be used in test.
- `KNIT_TEST_NAMESPACE`  : namespace to be used in test.

When you want to run tests in IDE, make sure that test environment is up in colima and `.testenv` is imported.
