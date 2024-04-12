Introduction
=======

Welcome to Knitfab!

This document covers the installation of Knitfab and a tutorial using a simple machine learning task.

他言語版/Translations
---------------------

- 日本語: [./getting-started.ja.md](./getting-started.ja.md)

What is Knitfab
------

Knitfab is a MLOps tool which has
- an automatic lineage management system
- with a tag-based workflow engine

Knitfab automatically executes those experiments once users Plan(Described later) them and gather the necessary data. During the execution, Knitfab records the inputs and outputs of each experiment, allowing user to trace the history.

This allows for the entire history of all experiments to be organically linked.

> [!Caution]
>
> This tutorial does not work with Apple Silicon Mac or ARM Machine.

### Concepts

#### "Machine Learning Task" in Knitfab

In Knitfab, a machine learning task (or program) is generalized as a process that takes inputs and produces outputs.

Its entity is a container that runs on Kubernetes.

#### Tag

Knitfab allows resources to be put with key-value metadata called Tag.

Resources that can be tagged can have any number of Tags.

#### Data

Everything that is input or output in a machine learning task is considered Data.

In Knitfab, Data is considered as a "directory with Tags".

Users can upload Data to Knitfab or put or remove Tags to existing Data.

The entity of Data is Kubernetes Persistent Volume Claims and Persistent Volumes.

#### Plan

A Plan defines "what inputs to provide to a machine learning task and what outputs to expect".

Machine learning tasks are configured as container images.

Input and output are file paths where Data are mounted and they can be put Tags.

The Tags on input determine the Data that can be assigned to input. Knitfab assignes the Data that has all the Tags of the input in the Plan as the input.

The Tags on output is automatically set as Tags for the output Data. Knitfab executes the Plan as a Run and immediately sets the output Tags when the output is obtained.
(Of course, it is also possible to manually set a different Tag for the output Data.)

#### Run

A Run is a concrete machine learning task that is executed within Knitfab.

A Run is generated according to the definition of a Plan. Knitfab checks each input (Tags on input) of the Plan and executes the Run when all the required Data is available. Users cannot directly execute a Run.

The entity of a Run is a Kubernetes Job.

Installing Knitfab on a local environment
------

This chapter desribes the steps to install Knitfab on your PC and "try it out".

> [!Warning]
>
> The Knitfab installed using the method introduced here is a simplified version and relies on Kubernetes for data storage. Therefore, there is a possibility of losing information (such as Data and lineage) if the Kubernetes pod that configures it is restarted.
>
If you intend to use Knitfab for production purposes, please follow the instructions in the admin-guide to set up Knitfab.

### Required Environment and Tools for Installation

The following are the required environment and tools:

- A freely available Kubernetes cluster

The following tools are required:

- bash
- helm
- curl
- wget

The installer script is written as a bash shell script.
curl and helm are used internally by the installer.

#### Creating a Temporary Kubernetes Cluster

This section introduces a method to create and destroy a Kubernetes cluster freely for experimentation purposes.

For example, you can use [minikube](https://minikube.sigs.k8s.io/docs/).

Minikube is a tool for building a local Kubernetes cluster. In other words, it allows you to create a Kubernetes cluster within your own computer. Additionally, this Kubernetes cluster is dedicated to you, so you can easily delete it when it is no longer needed.

To start a cluster using minikube, run the following command:

### Installation

1. Obtain the installer
2. Generate the default configuration file from the installer
3. Install

#### Obtain the installer

The installer can be found at https://github.com/opst/knitfab/installer/installer.sh.

Download it to a suitable directory.

```
mkdir -p ~/devel/knitfab-install
cd ~/devel/knitfab-install

wget -O installer.sh https://raw.githubusercontent.com/opst/knitfab/main/installer/installer.sh
chmod +x ./installer.sh
```

#### Generate the default configuration file

Regarding the downloaded installer,

```
./installer.sh --prepare
```

When you run it, the installation settings for Knitfab will be generated in the `./knitfab_install_settings` directory.
**This configuration is described as "not persisting the information managed by Knitfab".**
Therefore, if you delete or restart the pods that make up Knitfab, there may be inconsistencies or loss of information.
It is recommended to use it only temporarily.

#### Installing

Use the created installation settings to actually install Knitfab.

```
./installer.sh -n ${NAMESPACE} -s ./knitfab_install_settings
```

`${NAMESPACE}` is an optional parameter that allows you to specify the namespace for the Kubernetes cluster where Knitfab will be installed. Please choose a name according to the Kubernetes naming conventions, which only allow lowercase alphanumeric characters and hyphens ("-"). The name must start and end with an alphanumeric character.

The script will proceed with the installation of the necessary components in order.
In addition, a directory containing connection settings for this Knitfab will be generated at `./knitfab_install_settings/handouts`.

If the above completes without any errors, the installation is complete.

> [!Note]
>
> If you want to use a kubeconfig other than the default kubeconfig, you can provide it with the `--kubeconfig` flag.
>
> ```
> ./installer.sh --kubeconfig ${KUBECONFIG} -n ${NAMESPACE} -s ./knitfab_install_settings
> ```

### Uninstalling

The installer also generates an uninstaller (`uninstaller.sh`) inside the `./knitfab_install_settings` directory.
You can uninstall by executing this command.

```
./knitfab_install_settings/uninstaller.sh --hard
```

The `--hard` option means to destroy all Knitfab resources, including the database and image registry.


CLI Tool: knit
-----------------

Operations on Knitfab are performed through the CLI command `knit`.
Before proceeding with the following tutorials, you need to obtain the `knit` command.

The tool can be obtained from https://github.com/opst/knitfab/releases.
Please download the binary that matches your environment.

For example:

```
mkdir -p ~/.local/bin

VERSION=v1.0.0  # or release which you desired
OS=linux        # or windows, darwin
ARCH=arm64      # or amd64

wget -O ~/.local/bin/knit https://github.com/opst/knitfab/releases/download/${VERSION}/knit-${OS}-${ARCH}
chmod -x ~/.local/bin/knit

# and prepend ~/.local/bin to ${PATH}
```

Tutorial 1: Training Models with Knitfab
-------

This tutorial walks through a very simple experiment to introduce the functionality of Knitfab.

For detailed usage, please refer to the user guide.

### Prerequisites

This walkthrough assumes that you have access to an installed Knitfab and the following tools are installed. Please install them as needed.

- docker
- graphviz's dot command

#### Docker Configuration

Configure Docker.

Knitfab deploys a container image registry within the cluster.
This registry is private, allowing you to conduct experiments with custom images without publishing them to DockerHub or other public repositories.

However, in order to do this, you need to trust the CA certificate of this registry with the Docker command.
For more details, please refer to the Docker documentation: https://docs.docker.com/engine/security/certificates/#understand-the-configuration

To make Docker trust the TLS certificate used by Knitfab, do the following:

```
cp -r /path/to/handout/docker/certs.d /etc/docker/certs.d
```

> [!Caution]
>
> This operation has a global impact on the behavior of Docker on your system.
> If you are sharing your computer with multiple users, please obtain consent from other users in advance.

> [!Note]
>
> If you are running dockerd in a virtual environment such as colima or minikube,
> the following steps need to be performed within that virtual environment.

### Create a working directory

Create a directory to store files for your machine learning task project and navigate to it.

You can choose any directory you like, but for this example, let's name it `first-Knitfab-project`.

```
mkdir -p ~/devel/first-Knitfab-project
cd ~/devel/first-Knitfab-project
```

### Initializing the knit command

First, you need to import the connection information to Knitfab as the configuration for the knit CLI.

> [!Note]
>
> If you are trying to connect to a Knitfab with someone other than yourself as the administrator, please ask the administrator to provide you with the handout.

Import the `knitprofile` file included in the handout (`handout`) generated when you installed Knitfab.
Do the following:

```
knit init /path/to/handout/knitprofile
```

Now, with this directory, your work using knit will be performed connected to the Knitfab that generated this handout.

You are now ready to start using Knitfab.

### Uploading Data

This time, let's create a deep learning-based handwritten digit classifier using [QMNIST](https://github.com/facebookresearch/qmnist).
QMNIST is a handwritten digit dataset created by facebookresearch. It is an extension and refinement of the famous MNIST dataset.

To upload QMNIST to Knitfab, first download the QMNIST dataset mentioned above and store the images and labels in a directory as pairs. Download the training and test datasets as follows:

```
mkdir -p data/qmnist-train data/qmnist-test

wget -O data/qmnist-train/images.gz https://raw.githubusercontent.com/facebookresearch/qmnist/master/qmnist-train-images-idx3-ubyte.gz
wget -O data/qmnist-train/labels.gz https://raw.githubusercontent.com/facebookresearch/qmnist/master/qmnist-train-labels-idx2-int.gz

wget -O data/qmnist-test/images.gz https://raw.githubusercontent.com/facebookresearch/qmnist/master/qmnist-test-images-idx3-ubyte.gz
wget -O data/qmnist-test/labels.gz https://raw.githubusercontent.com/facebookresearch/qmnist/master/qmnist-test-labels-idx2-int.gz
```

Next, upload the training dataset to Knitfab as Data.

```
knit data push -t format:mnist -t mode:training -t type:dataset -t project:first-Knitfab -n ./data/qmnist-train
```

The meanings of each option are as follows:

- `-t`: Set a "Tag" for the Data
- `-n`: Register the directory name as a "Tag" with the key `name:...`

This registers the training dataset as Data in Knitfab.
At this time, what is displayed in the console is the metadata of the registered "Data".

```json
{
    "knitId": "11fbba05-1a7a-48d4-9751-32963d726f51",
    "tags": [
        "format:mnist",
        "knit#id:11fbba05-1a7a-48d4-9751-32963d726f51",
        "knit#timestamp:2024-03-22T08:13:39.39+00:00",
        "mode:training",
        "name:qmnist-train",
        "project:first-Knitfab",
        "type:dataset"
    ],
    "upstream": {
        "path": "/upload",
        "tags": [],
        "run": {
            "runId": "7dc0b8da-8949-4f69-be99-83a4786cac18",
            "status": "done",
            "updatedAt": "2024-03-22T08:13:39.39+00:00",
            "plan": {
                "planId": "4b48ceff-fbaf-4c47-b4e9-77bcc5eb8a41",
                "name": "knit#uploaded"
            }
        }
    },
    "downstreams": [],
    "nomination": []
}
```

The value of the key `"knitId"` is the ID that identifies this Data. The same value is also registered as the value of the tag `knit#id`.

### Write a program to execute the machine learning task

A sample script for training QMNIST is prepared in `./scripts/train.py`, so let's use it.

This script is written using PyTorch and trains a deep learning model shown below with the training Data of QMNIST.

```python
class MyMnistModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.layers = torch.nn.Sequential(
            torch.nn.Conv2d(1, 16, kernel_size=3, padding=1),  # 1x28x28 -> 16x28x28
            torch.nn.ReLU(),
            torch.nn.Conv2d(16, 32, kernel_size=3, padding=1),  # 16x28x28 -> 32x28x28
            torch.nn.ReLU(),
            torch.nn.Flatten(),
            torch.nn.Linear(32 * 28 * 28, 1024),
            torch.nn.ReLU(),
            torch.nn.Linear(1024, 10),
        )

    def forward(self, x):
        logit = self.layers(x)
        return logit
```

The first goal is to run this as the machine learning task on Knitfab.

Note that this training script has the following configurations:

- The random seed is fixed to `0`.
    - This applies to the standard library, numpy, and pytorch.
- The training data consists of 60,000 image-label pairs, of which 50,000 are randomly selected for training and the rest for validation.
- Mini-batch training is performed with a batch size of 64.
- Additionally, the training is set to run for 3 epochs.

#### Local Verification

Before running it on Knitfab, let's first run this outside of Knitfab to see what happens.

This tutorial also includes a Dockerfile bundled to build this training script as a Docker image.
Let's validate the operation as a Docker container. You can build the image for the training script with the following command:

```
docker build -t knitfab-first-train:v1.0 -f scripts/train/Dockerfile ./scripts
```

This command builds a Docker image that can execute `./scripts/train.py` and sets it the alias (tag) `Knitfab-first-train:v1.0`.

The contents of the Dockerfile are as follows:

```Dockerfile
FROM python:3.11

WORKDIR /work

RUN pip install numpy==1.26.4 && \
    pip install torch==2.2.1 --index-url https://download.pytorch.org/whl/cpu

COPY . .

ENTRYPOINT [ "python", "-u", "train.py" ]
CMD [ "--dataset", "/in/dataset", "--save-to", "/out/model" ]
```

This Dockerfile installs the dependent libraries and runs `./train.py`. It specifies the CPU version of PyTorch because it is not intended to be executed in a GPU environment.
`train.py` takes two command-line flags:

- `--dataset /in/dataset`: The location of the training dataset is `/in/dataset` inside the container.
- `--save-to /out/model`: The destination to save the model parameters is `/out/model` inside the container.

Therefore, to run it, you need to mount the dataset and model output directories to these two file paths.

```
mkdir -p ./out/model

docker run --rm -it \
    -v "$(pwd)/data/qmnist-train:/in/dataset" \
    -v "$(pwd)/out/model:/out/model" \
    knitfab-first-train:v1.0
```

On the host machine, the model is saved to `./out/model`.
In the container, the directory where the QMNIST training dataset was downloaded is specified as `/in/dataset`.
If you see the following log, it means the operation was successful.

```
data shape:(60000, 28, 28), type: uint8
label shape:(60000,), type: uint8
**TRAINING START** Epoch: #1
Epoch: #1, Batch: #0 -- Loss: 2.3024802207946777, Accuracy: 0.046875
Epoch: #1, Batch: #100 -- Loss: 2.154975175857544, Accuracy: 0.29842202970297027
Epoch: #1, Batch: #200 -- Loss: 0.667496919631958, Accuracy: 0.5030317164179104
Epoch: #1, Batch: #300 -- Loss: 0.3974001109600067, Accuracy: 0.6195494186046512
Epoch: #1, Batch: #400 -- Loss: 0.2097681164741516, Accuracy: 0.6856296758104738
Epoch: #1, Batch: #500 -- Loss: 0.3507159948348999, Accuracy: 0.7278255988023952
Epoch: #1, Batch: #600 -- Loss: 0.18445907533168793, Accuracy: 0.7567595673876872
Epoch: #1, Batch: #700 -- Loss: 0.31259363889694214, Accuracy: 0.7791993580599144
**TRAINING RESULT** Epoch: #1 -- total Loss: 597.214958243072, Accuracy: 0.79342
**VALIDATION START** Epoch: #1
**VALIDATION RESULT** Epoch: #1 -- total Loss: 44.73237031698227, Accuracy: 0.9127
**SAVING MODEL** at Epoch: #1
**TRAINING START** Epoch: #2
...(snip)...
```

As a confirmation of the operation, it is sufficient to observe just one epoch, so let's interrupt (`Ctrl+C`).

> [!Note]
>
> Warning messages related to OpenBLAS may be displayed at the beginning of the log, but you can ignore them.

### Registering a Machine Learning Task with Knitfab

Once you have confirmed the operation, let's use it to delegate a machine learning task to Knitfab.

This involves two steps:

1. Register the Docker image in the Knitfab cluster's internal image registry.
2. Create a Plan definition from the image and register it with Knitfab.

#### Registering the Docker Image with Knitfab

Let's set a new "Tag" for the Docker image you created earlier.

```
docker tag Knitfab-first-train:v1.0 ${YOUR_Knitfab_NODE}:${PORT}/knitfab-first-train:v1.0
```

Please specify the IP address of any node in your Knitfab cluster as `${YOUR_Knitfab_NODE}`.

> You can find the IP address of the node using commands like the following:
>
> ```
> kubectl get node -o wide
> ```
>

`${PORT}` is the port number of the image registry. By default, it should be `30503`.

If the image has been tagged, you can now push it to the registry.

```
docker push ${YOUR_Knitfab_NODE}:${PORT}/knitfab-first-train:v1.0
```

#### Creating a Plan definition from the image and registering it with Knitfab

Then, you need to communicate to Knitfab how you want to use the Docker image that you pushed using `docker push`.
To do this, let's create a definition for a Plan and send it to Knitfab.

You can generate a template for the Plan definition using the `knit` command.

```
docker save ${YOUR_Knitfab_NODE}:${PORT}/knitfab-first-train:v1.0 | \
    knit plan template > ./knitfab-first-train.v1.0.plan.yaml
```

> [!Note]
>
> The image is a bit large (1GB+) so it may take some time.

The `knit plan template` command analyzes the Docker image and generates a template for the Plan definition.
The following file should have been generated as `./knitfab-first-train.v1.0.plan.yaml`.

```yaml
# image:
#   Container image to be executed as this plan.
#   This image-tag should be accessible from your Knitfab cluster.
image: "${YOUR_Knitfab_NODE}:${PORT}/knitfab-first-train:v1.0"

# inputs:
#   List of filepath and tags as input of this plans.
#   1 or more inputs are needed.
#   Each filepath should be absolute. Tags should be formatted in "key:value"-style.
inputs:
  - path: "/in/dataset"
    tags:
      - "type:dataset"

# outputs:
#   List of filepathes and tags as output of this plans.
#   See "inputs" for detail.
outputs:
  - path: "/out/model"
    tags:
      - "type:model"

# log (optional):
#   Set tags stored log (STDOUT+STDERR of runs of this plan) as data.
#   If missing or null, log would not be stored.
log:
  tags:
    - "type:log"

# active (optional):
#   To suspend executing runs by this plan, set false explicitly.
#   If missing or null, it is assumed as true.
active: true

# resource (optional):
# Specify the resource , cpu or memory for example, requirements for this plan.
# This value can be changed after the plan is applied.

# There can be other resources. For them, ask your administrator.

# (advanced note: These values are passed to container.resource.limits in kubernetes.)
resouces:

  # cpu (optional; default = 1):
  #   Specify the CPU resource requirements for this plan.
  #   This value means "how many cores" the plan will use.
  #   This can be a fraction, like "0.5" or "500m" (= 500 millicore) for half a core.
  cpu: 1

  # memory (optional; default = 1Gi):
  #   Specify the memory resource requirements for this plan.
  #   This value means "how many bytes" the plan will use.
  #   You can use suffixes like "Ki", "Mi", "Gi" for kibi-(1024), mebi-(1024^2), gibi-(1024^3) bytes, case sensitive.
  #   For example, "1Gi" means 1 gibibyte.
  #   If you omit the suffix, it is assumed as bytes.
  memory: 1Gi


# # on_node (optional):
# #   Specify the node where this plan is executed.
# #
# #   For each level (may, prefer and must), you can put node labels or taints in "key=value" format.
# #   Labels show a node characteristic, and taints show a node restriction.
# #   Ask your administrator for the available labels/taints.
# #
# #   By default (= empty), this plan is executed on any node, if the node does not taint.
# on_node:
#   # may: (optional)
#   #   Allow to execute this plan on nodes with these taints, put here.
#   may:
#     - "label-a=value1"
#     - "label-b=value2"
#
#   # prefer: (optional)
#   #   Execute this plan on nodes with these labels & taints, if possible.
#   prefer:
#     - "vram=large"
#
#   # must: (optional)
#   #   Always execute this plan on nodes with these labels & taints
#   #   (taints on node can be subset of this list).
#   #
#   #   If no node matches, runs of the plan will be scheduled but not started.
#   must:
#     - "accelarator=gpu"
```

There are some parts that are not correct, so let's make corrections.

- Replace `${YOUR_Knitfab_NODE}` in the image name with `localhost`
  - This means that the image is located at `localhost` for Knitfab to execute your container.
- Add the following "tags" to the input `/in/dataset`:
    - `"project:first-Knitfab"`
    - `"mode:training"`
- Add the following "tags" to the output `/out/model`:
    - `"project:first-Knitfab"`
    - `"description:2 layer CNN + 2 layer Affine"`
- Add the following "tags" to the log:
    - `"project:first-Knitfab"`

On the input side the "tags" are specified the same "tags" as the Data that was pushed with `knit data push` to use it.
On the output side, the project name (`project`) and a brief description of the model (`description`) are written to record what is being output.

As a whole, the following Plan definition is obtained. The irrelevant commented-out parts have been removed.

```yaml
# image:
#   Container image to be executed as this plan.
#   This image-tag should be accessible from your Knitfab cluster.
image: "localhost:${PORT}/knitfab-first-train:v1.0"

# inputs:
#   List of filepath and tags as input of this plans.
#   1 or more inputs are needed.
#   Each filepath should be absolute. Tags should be formatted in "key:value"-style.
inputs:
  - path: "/in/dataset"
    tags:
      - "project:first-Knitfab"
      - "type:dataset"
      - "mode:training"

# outputs:
#   List of filepathes and tags as output of this plans.
#   See "inputs" for detail.
outputs:
  - path: "/out/model"
    tags:
      - "project:first-Knitfab"
      - "type:model"
      - "description: 2 layer CNN + 2 layer Affine"

# log (optional):
#   Set tags stored log (STDOUT+STDERR of runs of this plan) as data.
#   If missing or null, log would not be stored.
log:
  tags:
    - "project:first-Knitfab"
    - "type:log"

# active (optional):
#   To suspend executing runs by this plan, set false explicitly.
#   If missing or null, it is assumed as true.
active: true

# resource (optional):
# Specify the resource , cpu or memory for example, requirements for this plan.
# This value can be changed after the plan is applied.

# There can be other resources. For them, ask your administrator.

# (advanced note: These values are passed to container.resource.limits in kubernetes.)
resouces:

  # cpu (optional; default = 1):
  #   Specify the CPU resource requirements for this plan.
  #   This value means "how many cores" the plan will use.
  #   This can be a fraction, like "0.5" or "500m" (= 500 millicore) for half a core.
  cpu: 1

  # memory (optional; default = 1Gi):
  #   Specify the memory resource requirements for this plan.
  #   This value means "how many bytes" the plan will use.
  #   You can use suffixes like "Ki", "Mi", "Gi" for kibi-(1024), mebi-(1024^2), gibi-(1024^3) bytes, case sensitive.
  #   For example, "1Gi" means 1 gibibyte.
  #   If you omit the suffix, it is assumed as bytes.
  memory: 1Gi
```

Send this to Knitfab with the following command.

```
knit plan apply ./knitfab-first-train.v1.0.plan.yaml
```

Then, the information of the registered Plan should be displayed. It should have the following content:

```json
{
    "planId": "2a701485-2194-4503-8a09-1916bba7e5d1",
    "image": "localhost:30503/knitfab-first-train:v1.0",
    "inputs": [
        {
            "path": "/in/dataset",
            "tags": [
                "mode:training",
                "project:first-Knitfab",
                "type:dataset"
            ]
        }
    ],
    "outputs": [
        {
            "path": "/out/model",
            "tags": [
                "description:2 layer CNN + 2 layer Affine",
                "project:first-Knitfab",
                "type:model"
            ]
        }
    ],
    "log": {
        "Tags": [
            "project:first-Knitfab",
            "type:log"
        ]
    },
    "active": true,
    "resources": {
        "cpu": "1",
        "memory": "1Gi"
    }
}
```

The key `planId` is the unique identifier for this Plan.

### Wait

Once reached this point, all you have to do is wait.

Occasionally exwcute `knit run find -p ${PLAN_ID}` to monitor the generation of the Run and observe its changing status.
Please specify the planId included in the result of `knit plan apply` as `${PLAN_ID}`.

You will receive console output similar to the following:

```json
[
    {
        "runId": "6ece5f38-7b53-41a9-a3bc-653b89d37566",
        "status": "running",
        "updatedAt": "2024-03-22T09:10:08.084+00:00",
        "plan": {
            "planId": "2a701485-2194-4503-8a09-1916bba7e5d1",
            "image": "localhost:30503/knitfab-first-train:v1.0"
        },
        "inputs": [
            {
                "path": "/in/dataset",
                "tags": [
                    "mode:training",
                    "project:first-Knitfab",
                    "type:dataset"
                ],
                "knitId": "11fbba05-1a7a-48d4-9751-32963d726f51"
            }
        ],
        "outputs": [
            {
                "path": "/out/model",
                "tags": [
                    "description:2 layer CNN + 2 layer Affine",
                    "project:first-Knitfab",
                    "type:model"
                ],
                "knitId": "cf547e09-5aa6-4755-b66a-6676d9725ab4"
            }
        ],
        "log": {
            "Tags": [
                "project:first-Knitfab",
                "type:log"
            ],
            "knitId": "3b0eff13-a48f-4cbb-aa62-cd2c4e3e9a54"
        }
    }
]
```

Among these, the key `runId` uniquely identifies this Run.

If the `status` is `running` as shown in the example above, this Run is in progress.

You can check the training logs with the following command.

```
knit run show --log ${RUN_ID}
```

The value `${RUN_ID}` is the runId found with `knit run find`. This command displays the log for the specified Run ID.

### Download the Model

Download the trained model to your local machine.

Please check the status of the Run again and make sure it is `"status": "done"`.

```
knit run show ${RUN_ID}
```

Now, the following content should be written to the console.

```json
{
    "runId": "6ece5f38-7b53-41a9-a3bc-653b89d37566",
    "status": "done",
    "updatedAt": "2024-03-22T09:37:32.923+00:00",
    "exit": {
        "code": 0,
        "message": "Completed"
    },
    "plan": {
        "planId": "2a701485-2194-4503-8a09-1916bba7e5d1",
        "image": "localhost:30503/knitfab-first-train:v1.0"
    },
    "inputs": [
        {
            "path": "/in/dataset",
            "tags": [
                "mode:training",
                "project:first-Knitfab",
                "type:dataset"
            ],
            "knitId": "11fbba05-1a7a-48d4-9751-32963d726f51"
        }
    ],
    "outputs": [
        {
            "path": "/out/model",
            "tags": [
                "description:2 layer CNN + 2 layer Affine",
                "project:first-Knitfab",
                "type:model"
            ],
            "knitId": "cf547e09-5aa6-4755-b66a-6676d9725ab4"
        }
    ],
    "log": {
        "Tags": [
            "project:first-Knitfab",
            "type:log"
        ],
        "knitId": "3b0eff13-a48f-4cbb-aa62-cd2c4e3e9a54"
    }
}
```

Among these, the content written under `outputs` represents the Data that this Run actually generated.
The `knitId` indicates the unique ID that identifies the Data within Knitfab.

The output that wrote out the model was `"path": "/out/model"`.
Specify the `knitId` and download the model as Data.

```
mkdir -p ./knitfab/out/model
knit data pull -x ${KNIT_ID} ./knitfab/out/model
```

By doing this, the content of the outputted Data will be written to the directory `./knitfab/out/model/${KNIT_ID}`.

Tutorial 2: Evaluating Model Performance
------------------

### Required Tools

In this section, we will use `dot` (graphviz).
Please install it if necessary.

### Verification of the Evaluation Script

You can use `./scripts/validation.py` to perform inference using the model. You can also build a command launch image using the `validation/Dockerfile`.

```
docker build -t Knitfab-first-validation:v1.0 -f ./scripts/validation/Dockerfile ./scripts
```

The content of this Dockerfile is as follows:

```Dockerfile
FROM python:3.11

WORKDIR /work

RUN pip install numpy==1.26.4 && \
    pip install torch==2.2.1 --index-url https://download.pytorch.org/whl/cpu

COPY . .

ENTRYPOINT [ "python", "-u", "validation.py", "--dataset", "/in/dataset", "--model", "/in/model/model.pth" ]
```

It is similar to the training side. The differences are:

- The script file to execute is named `validation.py`.
    - This is the evaluation script.
- The command-line flag `--save-to` is removed and replaced with `--model`.
    - It reads the model from this file path.

Furthermore, `validation.py` can be passed an argument called `--id` to perform inference only on the image with that image number.

Let's first use this image to see if the inference is working correctly.
To mount the evaluation dataset and model and see how it works, you can execute the following command.

```
docker run -it --rm -v "$(pwd)/data/qmnist-test:/in/dataset" -v "$(pwd)/knitfab/out/model/${KNIT_ID}:/in/model" Knitfab-first-validation:v1.0 --id IMAGE_ID
```


(Please replace `${KNIT_ID}` with the appropriate value for your environment)

For example, if you set `--id 1`,

```
img shape torch.Size([60000, 28, 28])
label shape torch.Size([60000])
=== image ===



            ####
         ########
        #########
        ###    ###
        ##     ##
              ###
              ###
             ###
            ####
           ####
           ###
          ####
          ###
         ####
        ####
        ###
        ###           ####
        ##################
        ################
             #####





=== ===== ===
Prediction: tensor([2]), Ground Truth: 2
```

You will obtain results like above. The image with that ID will be displayed as ASCII art, followed by the prediction and ground truth.

In the above example, both the image content and the predicted and ground truth values of the model match with "2", indicating that the inference is correct.

Next, let's try evaluating this model with the test dataset using Knitfab.

The steps are similar to the training phase:

- Register the dataset in Knitfab using `knit data push`
- Push the image using `docker push`
- Create and register the Plan definition in Knitfab using `knit plan apply`

### Register the dataset

Let's register the test dataset as a Data in Knitfab.

The dataset has already been downloaded, so all need to do is register it.

```
knit data push -t format:mnist -t mode:test -t type:dataset -t project:first-Knitfab -n ./data/qmnist-test
```

### Push the evaluation image

Since the build has been done earlier, you just need to set the tag for the Knitfab cluster registry and `docker push` it.

```
docker tag Knitfab-first-validation:v1.0 ${YOUR_Knitfab_NODE}:${PORT}/knitfab-first-validation:v1.0

docker push ${YOUR_Knitfab_NODE}:${PORT}/knitfab-first-validation:v1.0
```

### Create and register the Plan

Let's obtain the template for the Plan based on the created image.

```
docker save ${YOUR_Knitfab_NODE}:${PORT}/knitfab-first-validation:v1.0 | knit plan template > ./knitfab-first-validation.v1.0.plan.yaml
```

You will obtain a file with the following contents.

```yaml
# image:
#   Container image to be executed as this plan.
#   This image-tag should be accessible from your Knitfab cluster.
image: "${YOUR_Knitfab_NODE}:${PORT}/knitfab-first-validation:v1.0"

# inputs:
#   List of filepath and tags as input of this plans.
#   1 or more inputs are needed.
#   Each filepath should be absolute. Tags should be formatted in "key:value"-style.
inputs:
  - path: "/in/dataset"
    tags:
      - "type:dataset"
  - path: "/in/model/model.pth"
    tags:
      - "type:model.pth"

# outputs:
#   List of filepathes and tags as output of this plans.
#   See "inputs" for detail.
outputs: []

# log (optional):
#   Set tags stored log (STDOUT+STDERR of runs of this plan) as data.
#   If missing or null, log would not be stored.
log:
  tags:
    - "type:log"

# active (optional):
#   To suspend executing runs by this plan, set false explicitly.
#   If missing or null, it is assumed as true.
active: true

# resource (optional):
# Specify the resource , cpu or memory for example, requirements for this plan.
# This value can be changed after the plan is applied.

# There can be other resources. For them, ask your administrator.

# (advanced note: These values are passed to container.resource.limits in kubernetes.)
resouces:
  # cpu (optional; default = 1):
  #   Specify the CPU resource requirements for this plan.
  #   This value means "how many cores" the plan will use.
  #   This can be a fraction, like "0.5" or "500m" (= 500 millicore) for half a core.
  cpu: 1

  # memory (optional; default = 1Gi):
  #   Specify the memory resource requirements for this plan.
  #   This value means "how many bytes" the plan will use.
  #   You can use suffixes like "Ki", "Mi", "Gi" for kibi-(1024), mebi-(1024^2), gibi-(1024^3) bytes, case sensitive.
  #   For example, "1Gi" means 1 gibibyte.
  #   If you omit the suffix, it is assumed as bytes.
  memory: 1Gi
# # on_node (optional):
# #   Specify the node where this plan is executed.
# #
# #   For each level (may, prefer and must), you can put node labels or taints in "key=value" format.
# #   Labels show a node characteristic, and taints show a node restriction.
# #   Ask your administrator for the available labels/taints.
# #
# #   By default (= empty), this plan is executed on any node, if the node does not taint.
# on_node:
#   # may: (optional)
#   #   Allow to execute this plan on nodes with these taints, put here.
#   may:
#     - "label-a=value1"
#     - "label-b=value2"
#
#   # prefer: (optional)
#   #   Execute this plan on nodes with these labels & taints, if possible.
#   prefer:
#     - "vram=large"
#
#   # must: (optional)
#   #   Always execute this plan on nodes with these labels & taints
#   #   (taints on node can be subset of this list).
#   #
#   #   If no node matches, runs of the plan will be scheduled but not started.
#   must:
#     - "accelarator=gpu"
```

Let's correct this and make it meaningful.

- Replace `${YOUR_Knitfab_NODE}` in the image name with `localhost`.
- Regarding the first input:
    - Add the following tags:
        - `"mode:test"`
        - `"project:first-Knitfab"`
- The second input is incorrect.
    - The `path` should specify a directory. Remove the file name.
    - Add/modify tags to include the Data with trained model parameters.
        - `"type:model.pth"` -> `"type:model"`
        - `"project:first-Knitfab"`
- Regarding the log:
    - Add the following tags:
        - `"project:first-Knitfab"`
        - `"type:validation"`

Overall, it should look like this:

```yaml
# image:
#   Container image to be executed as this plan.
#   This image-tag should be accessible from your Knitfab cluster.
image: "localhost:${PORT}/knitfab-first-validation:v1.0"

# inputs:
#   List of filepath and tags as input of this plans.
#   1 or more inputs are needed.
#   Each filepath should be absolute. Tags should be formatted in "key:value"-style.
inputs:
  - path: "/in/dataset"
    tags:
      - "type:dataset"
      - "mode:test"
      - "project:first-Knitfab"
  - path: "/in/model"
    tags:
      - "type:model"
      - "project:first-Knitfab"

# outputs:
#   List of filepathes and tags as output of this plans.
#   See "inputs" for detail.
outputs: []

# log (optional):
#   Set tags stored log (STDOUT+STDERR of runs of this plan) as data.
#   If missing or null, log would not be stored.
log:
  tags:
    - "type:log"
    - "type:validation"
    - "project:first-Knitfab"

# active (optional):
#   To suspend executing runs by this plan, set false explicitly.
#   If missing or null, it is assumed as true.
active: true

# resource (optional):
# Specify the resource , cpu or memory for example, requirements for this plan.
# This value can be changed after the plan is applied.

# There can be other resources. For them, ask your administrator.

# (advanced note: These values are passed to container.resource.limits in kubernetes.)
resouces:
  # cpu (optional; default = 1):
  #   Specify the CPU resource requirements for this plan.
  #   This value means "how many cores" the plan will use.
  #   This can be a fraction, like "0.5" or "500m" (= 500 millicore) for half a core.
  cpu: 1

  # memory (optional; default = 1Gi):
  #   Specify the memory resource requirements for this plan.
  #   This value means "how many bytes" the plan will use.
  #   You can use suffixes like "Ki", "Mi", "Gi" for kibi-(1024), mebi-(1024^2), gibi-(1024^3) bytes, case sensitive.
  #   For example, "1Gi" means 1 gibibyte.
  #   If you omit the suffix, it is assumed as bytes.
  memory: 1Gi
```

Register this content with Knitfab.

```
knit plan apply ./knitfab-first-validation.v1.0.plan.yaml
```

Then, Knitfab will generate and execute a Run based on the combination of the model parameters generated earlier and the newly specified dataset.

- Monitor the progress with `knit run find -p ${PLAN_ID}`.
- View the logs with `knit run show --log ${RUN_ID}`.

Eventually, the Run performing the evaluation will have a `"status": "done"`.
Read the logs again to confirm that the training was successful.

```
Accuracy (at 10000 images): 0.9629
Accuracy (at 20000 images): 0.96095
Accuracy (at 30000 images): 0.9602
Accuracy (at 40000 images): 0.9604
Accuracy (at 50000 images): 0.95974
Accuracy (at 60000 images): 0.95985

=== Validation Result ===
Accuracy: 0.95985
```

Tutorial 3: Exploring Lineage
---------------

Finally, let's examine the lineage generated by the experiments so far.

You can investigate the entire lineage related to a Data using the following command.

```
knit data lineage -n all ${KNIT_ID} | dot -T png -o ./lineage-graph.png
```


The `knit data lineage` command generates a lineage graph in dot format, starting from the specified `${KNIT_ID}`.

By passing this command to the `dot` command of graphviz and exporting it as a png file, you can observe the lineage graph as the following image.

![lineage graph](images/lineage.png)

Summary
-------

With this, the content of this book is concluded.

> [!Note]
>
> If necessary, please uninstall/destroy Knitfab and the Kubernetes cluster.

The topics covered in this book are:

- Conducted a simple installation of Knitfab.
- Trained a model using Knitfab.
- Evaluated a model using Knitfab.
- With Knitfab, both model training and evaluation are automatically performed by simply registering Data and Plan.

For further details, please refer to the user guide and operation guide.
