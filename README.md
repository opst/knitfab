Knitfab
=======

![Knitfab logo](./logo.png)

MLOps system & tool. Release AI/ML engineers from trivial routines.

- automatic lineage tracking
- tag based dinamic workflow
- using k8s & container to isolate ML tasks

directory layout
===================

- `build/`: build scripts
- `docs`: documents
- `cmd/`: commands, say entry points of programmes
    - `dataagt` : single function web server -- read/write data into a directory
        - started as k8s pod/service, used for create/read from Persistent Volume of k8s.
    - `knit` : cli of Knitfab api
    - `knit_nurse` : run as sidecar of Knitfab worker to track logs
    - `knitd` : frontend API server of Knitfab, facing user request (via knitfab command)
    - `knitd_backend` : internal server of Knitfab, using k8s api.
    - `volume_expander` : as k8s DaemonSet, watch k8s PVC and resize it
- `charts/`: helm chart creates test environment
    - `src/`: sources (meta-template) of chart.
    - `release/` : released charts
    - `local/` : charts for local trial in dev-cluster (created when local build)
    - `test/` : charts for automated test (created when test)
- `dev-cluster/`: provisioner of local virtual k8s cluster
- `installer/`:
    - `installer.sh`: web-based installer script.
- `internal/`: internal golang packages
- `pkg/`: golang packages
- `go.{mod,sum}`
- `README.md` : this file
- `LICENSE`: BUSL-1.1
- `testctl.sh` : test environment provisioner & test runner

Getting Started
================

Read [docs/01.getting-started](docs/01.getting-started) .

For more detail, see [docs/02.user-guide](docs/02.user-guide)

How to Install
==============

Read [docs/03.admin-guide](docs/03.admin-guide) .

Build Knitfab
==============

Our build script is `./build/build.sh` .
This script is build Knitfab into installer bundle.

To build, just run

```
./build/build.sh
```

### usage

```
# build images and charts
./build/build.sh [--debug] [--release] [--test]

# generate debug configuration for IDE
./build/build.sh --ide vscode
```

### build images and charts

```
./build/build.sh [--debug] [--release] [--test]

OPTIONS:

    --debug       do "debug build"
    --release     do "release build"
    --test        do build for automated test
```

`--release` and `--test` are exclusive.
When neither `--release` nor `--test` are passed, it performs local build (for dev-cluster).

`./build/build.sh` requires commands below:

- bash
- docker and docker-compose (or docker compose) with buildkit
- base64
- git
- envsubst

### debug build

When `./build/build.sh --debug`, it generates "debug mode" installer.

Debug mode installer has extra items & features.

- images execute Knitfab comoponents with [`dlv exec`](https://github.com/go-delve/delve)
- k8s services routing to `dlv` debugger server

So, when you deploy Knitfab from "debug mode" installer,
the cluster exposes port (below) to be attached with `dlv` and compatible IDE.

- knitd: `<any cluster node>:30990`
- knitd-backend : `<any cluster node>:30991`

You can get configurations for IDE to attach knitd/knitd-backend with `./build/build.sh --ide <IDE_NAME>`.

Currently, we supports `--ide vscode` only.

If IDEs you prefer is not supported, write your config and share us.

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

dev-cluster: k8s cluster for developer
==================================

This repository contains provisioning scripts to deploy local kubernetes cluster, based on **virtualbox**+**vagrant**+**ansible**.

Use the cluster to try or debug Knitfab.

### prerequirements

- [vagrant](https://www.vagrantup.com/docs/installation)
- [virtualbox](https://www.virtualbox.org/) : dependent of vagrant, to create virtual machiness
- [poetry](https://python-poetry.org/) : it is used to install ansible
- [python](https://www.python.org/) (3.8+) : dependent of poetry
- [docker, docker compose](https://docs.docker.com/) : build & push image
- (optional) [pyenv](https://github.com/pyenv/pyenv) : to manage your python installation

And, `dev-cluster` uses [ansible](https://docs.ansible.com/ansible/latest/index.html), but it will be installed by poetry.

### to start k8s cluster

To start, move to `dev-cluster` directry, and run

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

It takes 10+ minutes at least, and can be over 30 minutes. Be patient.
If you want to throw away them all, just do `vagrant destroy -f` and vms+k8s clusters are destroied.

### how is dev-cluster provisioned?

dev-cluster is a k8s cluster has nodes (VirtualBox VM) below:

- `knit-master` (default IP: `10.10.0.2`)
    - k8s master node.
- `knit-gateway` (default IP: `10.10.0.3`)
    - one of k8s worker node.
    - also it is NFS Server.
- `knit-nodes-${n}` (default IP: `10.10.0.4` or more)
    - k8s worker nodes.

During provisioning, it generates records of configurations of the cluster in `.sync` directory.

- `.sync/cert/{ca,server}.{crt,key}`: self signed certificate & key pair, certificates IP of `knit-gateway` node.
- `.sync/kubeconfig/kubeconfig`: kubeconfig
- `.sync/kubeadm`: output of `kubeadm token`
- `.sync/knit/install_setting`: Knitfab installation setting (it is used from `./dev-cluster/install-knit.sh`)

### install Knitfab into dev-cluster

1. Copy `./dev-cluster/.sync/certs/ca.crt` to your `/etc/docker/certs.d/${VM-KNIT-GATEWAY-IP}:${IMAGE-REGISTRY-PORT}` (only when dev-cluster recreating)
    - By default, `${VM-KNIT-GATEWAY-IP}:${IMAGE-REGITRY-PORT}` is `10.10.0.3:30503`.
    - `${VM-KNIT-GATEWAY-IP}` should be the ip address of VirtualBox vm instance "knit-gateway".
    - `${IMAGE-REGISTRY-PORT}` is defined in chart value `./charts/local/v*/knit-image-registry/values.yaml` .
2. Run `./build/build.sh`
3. Then run `./dev-cluster/install-knit.sh`
    - it generates `./configure/handout` directory, containing knitprofile file.
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

`./dev-cluster/knitctl.sh` is wrapper of `KUBECONFIG=... kubectl`.

So, you can just do

```
./dev-cluster/knitctl.sh ...
```

instead of

```
$ kubectl --kubeconfig ./dev-cluster/kubeconfig/kubeconfig ...
```

TEST ENVIRONMENT
-----------------

We provide a script, `./testctl.sh`, to create test environment.

It uses **colima** to create a k8s cluster for test.

This test envirionment is ISOLATED FROM `dev-cluster` because of scale and lifecycle.
Test environment should be *lightweight* to be created/disposed frequently. dev-cluster is too large.

It is recommended to expand memory of colima instance to 8GB+.
To do that, Update your `~/.colima/_template/default`.

To get up a new env, activate your colima, and run

```
./testctl.sh install
```

To test, run

```
./testctl.sh test
```

> [!Note]
>
> Kubernetes in colima VM and VirtualBox can conflict of networking.
>
> Sometimes you may need to suspend dev-cluster.

### environment variables

`./testctl.sh` saves env-vars in `.testenv` file.

- `KNIT_TEST_KUBECONFIG` : kubeconfig file to be used in test.
- `KNIT_TEST_KUBECTX`    : k8s context to be used in test.
- `KNIT_TEST_NAMESPACE`  : namespace to be used in test.

When you want to run tests in IDE, make sure that test environment is up in colima and `.testenv` is imported.
