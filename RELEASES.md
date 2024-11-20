v1.5.0
============

- Date: 2024-11-20

The v1.5.0 release includes

- `knit plan show` and `knit plan find` display direct upstreams/downstreams of each Plan
- New command `knit plan graph`
- Fix ambiguity of Data's upstream (breaking change)
- Fix case inconsistency in JSON (breaking change)

## Important Changes

### `knit plan show`and `knit plan find` display direct upstreams/downstreams of each Plan

These commands show upstream/downstream neighboring Plans of a Plan. As shown below:

```json
{
    "planId": ...
    ...
    "inputs": [
        {
            "path": "/in/1",
            "tags": [ ... ],
            "upstreams": [  // NEW!
              {
                  "plan": {
                      "planId": "...",
                      "image": "...",
                      "entrypoint": [ ... ],
                      "args": [ ... ],
                      "annotations": [ ... ]
                  },
                  "mountpoint": {  // this upstream is a normal output.
                      "path": "/out/1",
                      "tags": [ ... ]
                  }
              },
              {
                  "plan": {
                      "planId": "...",
                      "image": "...",
                      "entrypoint": [ ... ],
                      "args": [ ... ],
                      "annotations": [ ... ]
                  },
                  "log": {  // this upstream is a log.
                      "tags": [ ... ]
                  }
              },
            ]
        }
    ],
    "outputs": [
        {
            "path": "/out/1",
            "tags": [ ... ],
            "downstreams": [  // NEW!
                {
                    "plan": {
                        "planId": "...",
                        "image": "...",
                        "entrypoint": [ ... ],
                        "args": [ ... ],
                        "annotations": [ ... ]
                    },
                    "mountpoint": {
                        "path": "/in/2",
                        "tags": [ ... ]
                    }
                },
            ]
        }
    ],
    ...
}
```

Upstreams of a Plan Input are Plan Outputs or a Logs with matching Tags. If an Output (or Log) which has all Tags of the Input of another Plan, the Output is considered as Upstream of the Input. In other words, when Output or Log of a Plan A generates Data which can be assigned to an Input of Plan B, the Output/Log is upstream of the Input.

### `knit plan graph`

The new command `knit plan graph` generates Plan Graph, which is an overview of your Plans, in dot format.

`knit plan graph` traverse Plans upstream and/or downstream Plans recursively and visualize the "pipeline" made by Plans.

See `knit plan graph --help` for more details.

### Fix ambiguity of Data's upstream (breaking change)

Before this change, `"upstream"` of Data had not distinguished normal outputs and logs.
If the upstream of Data is a log, `knit data find` tells you the upstream has "path" (as `/log`).
However, the expression is identical to a that of normal output with the path `/log`, so with only `knit data find`, we could not know the upstream of Data is whether output or log.

By now, `knit data find` tells explicitly the upstream of Data is output or is log.

To do that, **breaking changes** are introduced. In JSON format of Data, `"path"` and `"tags"` are moved to a new field `"mountpoint"` (for normal outputs) or `"log"` (for log) .

```json
{
    "knitId": "...",
    "tags": [ ... ],
    "upstream": {
        "mountpoint": {  // NEW!
            "path": "/upload",
            "tags": []
        },
        "run": {
            "runId": " ... ",
            "status": "done",
            "updatedAt": "2024-11-18T04:25:32.076+00:00",
            "plan": { ... }
        }
    },
    "downstreams": [
        {
            "mountpoint": {  // NEW!
                "path": "/in/dataset",
                "tags": [
                    "mode:training",
                    "project:first-knitfab",
                    "type:dataset"
                ]
            },
            "run": {
                "runId": "b7ed ... ",
                "status": "running",
                "updatedAt": "2024-11-18T04:44:06.008+00:00",
                "plan": { ... }
            }
        }
    ],
    "nomination": [ ... ]
}
```

```json
{
    "knitId": "de17825d-16f3-4a5d-b3cb-16ef95379c0c",
    "tags": [
        "knit#id:de17825d-16f3-4a5d-b3cb-16ef95379c0c",
        "knit#timestamp:2024-11-18T05:12:32.075+00:00",
        "project:first-knitfab",
        "type:log"
    ],
    "upstream": {
        "log": {  // NEW!
            "tags": [
                "project:first-knitfab",
                "type:log"
            ]
        },
        "run": {
            "runId": "b7ed106b-cb49-4671-8979-4e85e249f15c",
            "status": "done",
            "updatedAt": "2024-11-18T05:12:32.075+00:00",
            "exit": {
                "code": 0,
                "message": ""
            },
            "plan": {
                "planId": "5770077f-e7a2-4b0f-8e8e-4d73d4b14144",
                "image": "localhost:30503/knitfab-first-train:v1.0",
                "entrypoint": [
                    "python",
                    "-u",
                    "train.py"
                ],
                "args": [
                    "--dataset",
                    "/in/dataset",
                    "--save-to",
                    "/out/model"
                ]
            }
        }
    },
    "downstreams": [],
    "nomination": []
}
```

Data with an `"upstream"` containing `"mountpoint"` represents a normal output, while one containing `"log"` represents a log.

### Fix case inconsistency in JSON (breaking change)

Plan had `log.Tag` field. Differently than other fields, the first letter of the filed name is capitalized.

This inconcistency is not intended, so it has been fixed to `log.tag`.

```json
{
    "planId": " ... ",
    "image": " ... ",
    // ...
    "log": {
        "tags": [  // renamed from "Tags".
            "project:first-knitfab",
            "type:log"
        ],
        "downstreams": []
    },
    // ...
}
```

## License

Knitfab v1.5.0 is released under BSL 1.1, as written in the LICENSE file.

CHANGE DATE for `v1.5.x` is 2028-11-20.

Previous releases, `v1.4.x` or brefore, are not changed in their CHANGE DATE.

## Upgrade Path

This release has breaking change, so upgrade both Knitfab system and CLI.

### Knitfab System

Download the installer, and run

```
installer.sh --install
```

in the directory where you have installed Knitfab.

### CLI `knit`

Download from assets of this release.

v1.5.0-beta
============

- Date: 2024-11-18

The v1.5.0-beta release includes

- `knit plan show` and `knit plan find` display direct upstreams/downstreams of each Plan
- New command `knit plan graph`
- Fix ambiguity of Data's upstream (breaking change)
- Fix case inconsistency in JSON (breaking change)

## Important Changes

### `knit plan show`and `knit plan find` display direct upstreams/downstreams of each Plan

These commands show upstream/downstream neighboring Plans of a Plan. As shown below:

```json
{
    "planId": ...
    ...
    "inputs": [
        {
            "path": "/in/1",
            "tags": [ ... ],
            "upstreams": [  // NEW!
              {
                  "plan": {
                      "planId": "...",
                      "image": "...",
                      "entrypoint": [ ... ],
                      "args": [ ... ],
                      "annotations": [ ... ]
                  },
                  "mountpoint": {  // this upstream is a normal output.
                      "path": "/out/1",
                      "tags": [ ... ]
                  }
              },
              {
                  "plan": {
                      "planId": "...",
                      "image": "...",
                      "entrypoint": [ ... ],
                      "args": [ ... ],
                      "annotations": [ ... ]
                  },
                  "log": {  // this upstream is a log.
                      "tags": [ ... ]
                  }
              },
            ]
        }
    ],
    "outputs": [
        {
            "path": "/out/1",
            "tags": [ ... ],
            "downstreams": [  // NEW!
                {
                    "plan": {
                        "planId": "...",
                        "image": "...",
                        "entrypoint": [ ... ],
                        "args": [ ... ],
                        "annotations": [ ... ]
                    },
                    "mountpoint": {
                        "path": "/in/2",
                        "tags": [ ... ]
                    }
                },
            ]
        }
    ],
    ...
}
```

Upstreams of a Plan Input are Plan Outputs or a Logs with matching Tags. If an Output (or Log) which has all Tags of the Input of another Plan, the Output is considered as Upstream of the Input. In other words, when Output or Log of a Plan A generates Data which can be assigned to an Input of Plan B, the Output/Log is upstream of the Input.

### `knit plan graph`

The new command `knit plan graph` generates Plan Graph, which is an overview of your Plans, in dot format.

`knit plan graph` traverse Plans upstream and/or downstream Plans recursively and visualize the "pipeline" made by Plans.

See `knit plan graph --help` for more details.

### Fix ambiguity of Data's upstream (breaking change)

Before this change, `"upstream"` of Data had not distinguished normal outputs and logs.
If the upstream of Data is a log, `knit data find` tells you the upstream has "path" (as `/log`).
However, the expression is identical to a that of normal output with the path `/log`, so with only `knit data find`, we could not know the upstream of Data is whether output or log.

By now, `knit data find` tells explicitly the upstream of Data is output or is log.

To do that, **breaking changes** are introduced. In JSON format of Data, `"path"` and `"tags"` are moved to a new field `"mountpoint"` (for normal outputs) or `"log"` (for log) .

```json
{
    "knitId": "...",
    "tags": [ ... ],
    "upstream": {
        "mountpoint": {  // NEW!
            "path": "/upload",
            "tags": []
        },
        "run": {
            "runId": " ... ",
            "status": "done",
            "updatedAt": "2024-11-18T04:25:32.076+00:00",
            "plan": { ... }
        }
    },
    "downstreams": [
        {
            "mountpoint": {  // NEW!
                "path": "/in/dataset",
                "tags": [
                    "mode:training",
                    "project:first-knitfab",
                    "type:dataset"
                ]
            },
            "run": {
                "runId": "b7ed ... ",
                "status": "running",
                "updatedAt": "2024-11-18T04:44:06.008+00:00",
                "plan": { ... }
            }
        }
    ],
    "nomination": [ ... ]
}
```

```json
{
    "knitId": "de17825d-16f3-4a5d-b3cb-16ef95379c0c",
    "tags": [
        "knit#id:de17825d-16f3-4a5d-b3cb-16ef95379c0c",
        "knit#timestamp:2024-11-18T05:12:32.075+00:00",
        "project:first-knitfab",
        "type:log"
    ],
    "upstream": {
        "log": {  // NEW!
            "tags": [
                "project:first-knitfab",
                "type:log"
            ]
        },
        "run": {
            "runId": "b7ed106b-cb49-4671-8979-4e85e249f15c",
            "status": "done",
            "updatedAt": "2024-11-18T05:12:32.075+00:00",
            "exit": {
                "code": 0,
                "message": ""
            },
            "plan": {
                "planId": "5770077f-e7a2-4b0f-8e8e-4d73d4b14144",
                "image": "localhost:30503/knitfab-first-train:v1.0",
                "entrypoint": [
                    "python",
                    "-u",
                    "train.py"
                ],
                "args": [
                    "--dataset",
                    "/in/dataset",
                    "--save-to",
                    "/out/model"
                ]
            }
        }
    },
    "downstreams": [],
    "nomination": []
}
```

Data with an `"upstream"` containing `"mountpoint"` represents a normal output, while one containing `"log"` represents a log.

### Fix case inconsistency in JSON (breaking change)

Plan had `log.Tag` field. Differently than other fields, the first letter of the filed name is capitalized.

This inconcistency is not intended, so it has been fixed to `log.tag`.

```json
{
    "planId": " ... ",
    "image": " ... ",
    // ...
    "log": {
        "tags": [  // renamed from "Tags".
            "project:first-knitfab",
            "type:log"
        ],
        "downstreams": []
    },
    // ...
}
```

## How to try

This release containing breaking changes, so please be sure to update both the Knitfab system AND CLI to try.

### Knitfab System

Download the installer from branch [develop/v1.5.0](https://github.com/opst/knitfab/tree/develop/v1.5.0), and run

```
BRANCH=develop/v1.5.0 CHART_VERSION=v1.5.0-beta installer.sh --install
```

in the directory where you have installed Knitfab.

### CLI knit

Download from assets of this release.

v1.4.0
===========

- Date: 2024-10-25

The v1.4.0 release includes

- New fields on Plan
- Dynamic Environmental Variable Injection to Run via Lifecycle Hook

## Important Changes

### New Fields on Plan

We added new fields to Plan.

#### Annotation

Now, Plan can be "annotated".

```yaml
image: "example.com/train:v1.0"

annotation:
  - "key=value"
  - "description=annotation can have arbitrary text content"
  - "creator=takaoka.youta@opst.co.jp"

inputs:
  - ...

outputs:
  - ...

...
```

`annotation` is `key=value` formatted metadata of Plans itself.
They are not Tags, so Knitfab does not consider annotations when making new Runs.

You can use annotations to record arbitrary text content, for example, "What is the Plan?" or "Who is creator of the Plan?".

Annotations are mutable. To update them, use the `knit plan annotate` command.

#### Entrypoint and Args

```yaml
image: "example.com/train:v1.0"

entrypoint: ["python", "main.py"]
args: ["--data", "/in/1/data", "--output", "/out/1/model"]

inputs:
  - path: /in/1/data
    tags:
      - "format:image/png"
      - "mode:train"
      - "type:dataset"
      - "project:example"

outputs:
  - path: /out/1/model
    tags:
      - "framework:pytorch"
      - "type:model"
      - "project:example"

...
```

With this change, Plan can have `entrypoint` and `args` which override default behavior of the image.

`entrypoint` overrides `ENTRYPOINT` directive in Dockerfile, and `args` overrides `CMD` directive.

These are optional, and by default, image will run as defined in its Dockerfile.

#### Service Account


```yaml
image: "example.com/train:v1.0"

inputs:
  - ...

outputs:
  - ...

service_account: service_account_name

...
```

You can assign [ServiceAccount of Kubernetes](https://kubernetes.io/docs/concepts/security/service-accounts/) on Plans.

When a new Run based a Plan with `service_account` is created, the Pod of the Run is started with the ServiceAccount specified on the Plan.

Pods are started without ServiceAccount by default, which is sufficient for most Runs are self-contained within a Pod.

When your Run needs access to the Kubernetes API or cloud platforms managed services, this feature may be useful.

`service_account` is mutable. To update this, use `knit plan serviceaccount` command.

### Dynamic Environmental Variable Injection to Run via Lifecycle Hook

Before this change, Knitfab ignores response bodies from Lifecycle Hooks.

With this change, Knitfab uses the response body of "Before starting" Lifecycle hook, if able.

If the response has `Content-Type: application/json` and contains the following structure,
Knitfab use `knitfabExtension.env` as environmental variables for the starting Pod.

```ts
{
  // ...
  "knitfabExtension": {
    "env": { [key: string]: string }
  }
  // ...
}
```

(the type notation is in TypeScript syntax)

If the response is not `Content-Type: application/json` or does not contain the structure,
it is ignored as it was.

You can write Lifecycle Hooks which determine envvars by the Run.

### `knit data tag`: behavior change

#### new flag `--remove-key`

Now Tags of data can be removed by the key.

If you do below, all tags with key `example-key` are removed from Data identified with `SOME-KNIT-ID`.

```
knit data tag --remove-key example-key SOME-KNIT-ID
```

Unlike `--remove`, `--remove-key` does not consider the Value of Tag.
It is useful to change Tags with a very long note.

#### `knit data --remove ... --add ...`: remove and then add now

With this release, execution order of `--add`, `--remove` or `--remove-key` are changed.

Now, `--remove`s and `--remove-key`s are effected first, and then `--add`s are effected.

The new behavior (remove first, then add) allows "update" Tags in atomic operation.

Example: update long description,

```
knit data tag --remove-key description --add "description:very long note of the Data" SOME-KNIT-ID
```

Example: update Tag referred by Plans in atomic operation.

```
knit data tag --remove version:latest --add version:3 SOME-KNIT-ID
```

## License

Knitfab v1.4.0 is released under BSL 1.1, as written in the LICENSE file.

CHANGE DATE for `v1.4.x` is 2028-10-25.

Previous releases, `v1.3.x` or brefore, are not changed in their CHANGE DATE.

## Upgrade Path

### Knitfab System

Download the installer, and run

```
installer.sh --install
```

in the directory where you have installed Knitfab.

### CLI `knit`

Download from assets of this release.

v1.4.0-beta
===========

- Date: 2024-10-23

The v1.4.0-beta release includes

- New fields on Plan
- Dynamic Environmental Variable Injection to Run via Lifecycle Hook

## Important Changes

### New Fields on Plan

We added new fields to Plan.

#### Annotation

Now, Plan can be "annotated".

```yaml
image: "example.com/train:v1.0"

annotation:
  - "key=value"
  - "description=annotation can have arbitrary text content"
  - "creator=takaoka.youta@opst.co.jp"

inputs:
  - ...

outputs:
  - ...

...
```

`annotation` is `key=value` formatted metadata of Plans itself.
They are not Tags, so Knitfab does not consider annotations when making new Runs.

You can use annotations to record arbitrary text content, for example, "What is the Plan?" or "Who is creator of the Plan?".

Annotations are mutable. To update them, use the `knit plan annotate` command.

#### Entrypoint and Args

```yaml
image: "example.com/train:v1.0"

entrypoint: ["python", "main.py"]
args: ["--data", "/in/1/data", "--output", "/out/1/model"]

inputs:
  - path: /in/1/data
    tags:
      - "format:image/png"
      - "mode:train"
      - "type:dataset"
      - "project:example"

outputs:
  - path: /out/1/model
    tags:
      - "framework:pytorch"
      - "type:model"
      - "project:example"

...
```

With this change, Plan can have `entrypoint` and `args` which override default behavior of the image.

`entrypoint` overrides `ENTRYPOINT` directive in Dockerfile, and `args` overrides `CMD` directive.

These are optional, and by default, image will run as defined in its Dockerfile.

#### Service Account


```yaml
image: "example.com/train:v1.0"

inputs:
  - ...

outputs:
  - ...

service_account: service_account_name

...
```

You can assign [ServiceAccount of Kubernetes](https://kubernetes.io/docs/concepts/security/service-accounts/) on Plans.

When a new Run based a Plan with `service_account` is created, the Pod of the Run is started with the ServiceAccount specified on the Plan.

Pods are started without ServiceAccount by default, which is sufficient for most Runs are self-contained within a Pod.

When your Run needs access to the Kubernetes API or cloud platforms managed services, this feature may be useful.

`service_account` is mutable. To update this, use `knit plan serviceaccount` command.

### Dynamic Environmental Variable Injection to Run via Lifecycle Hook

Before this change, Knitfab ignores response bodies from Lifecycle Hooks.

With this change, Knitfab uses the response body of "Before starting" Lifecycle hook, if able.

If the response has `Content-Type: application/json` and contains the following structure,
Knitfab use `knitfabExtension.env` as environmental variables for the starting Pod.

```ts
{
  // ...
  "knitfabExtension": {
    "env": { [key: string]: string }
  }
  // ...
}
```

(the type notation is in TypeScript syntax)

If the response is not `Content-Type: application/json` or does not contain the structure,
it is ignored as it was.

You can write Lifecycle Hooks which determine envvars by the Run.

### `knit data tag`: behavior change

#### new flag `--remove-key`

Now Tags of data can be removed by the key.

If you do below, all tags with key `example-key` are removed from Data identified with `SOME-KNIT-ID`.

```
knit data tag --remove-key example-key SOME-KNIT-ID
```

Unlike `--remove`, `--remove-key` does not consider the Value of Tag.
It is useful to change Tags with a very long note.

#### `knit data --remove ... --add ...`: remove and then add now

With this release, execution order of `--add`, `--remove` or `--remove-key` are changed.

Now, `--remove`s and `--remove-key`s are effected first, and then `--add`s are effected.

The new behavior (remove first, then add) allows "update" Tags in atomic operation.

Example: update long description,

```
knit data tag --remove-key description --add "description:very long note of the Data" SOME-KNIT-ID
```

Example: update Tag referred by Plans in atomic operation.

```
knit data tag --remove version:latest --add version:3 SOME-KNIT-ID
```

## How to try

### Knitfab System

Download the installer from branch [develop/v1.4.0](https://github.com/opst/knitfab/tree/develop/v1.4.0), and run

BRANCH=develop/v1.4.0 CHART_VERSION=v1.4.0-beta installer.sh --install


in the directory where you have installed Knitfab.

### CLI knit

Download from assets of this release.

v1.3.1
===========

- Date: 2024-09-30

The v1.3.1 release includes repository splitting for better ecosystem.

## Important Changes

### Documents are moved

Documents which were in the directory `./docs` are moved to the repository https://github.com/opst/knitfab-docs .

With this change, documents can be updated without Knitfab upgrades.

### WebAPI related types are moved

Type definitions related to Knitfab WebAPI, were in the directory `./pkg/api/types`, are moved to the repository https://github.com/opst/knitfab-api-types as [golang module](https://pkg.go.dev/github.com/opst/knitfab-api-types).

You can `go get opst/knitfab-api-types` and use it to build your Knitfab extensions.

## License

There are no changes in License.

## Upgrade Path

### Knitfab System

Download the installer, and run

```
installer.sh --install
```

in the directory where you have installed Knitfab.

### CLI `knit`

Download from assets of this release.

v1.3.1-beta
===========

- Date: 2024-09-26

The v1.3.1-beta release includes repository splitting for better ecosystem.

## Important Changes

### Documents are moved

Documents which were in the directory `./docs` are moved to the repository https://github.com/opst/knitfab-docs .

With this change, documents can be updated without Knitfab upgrades.

### WebAPI related types are moved

Type definitions related to Knitfab WebAPI, were in the directory `./pkg/api/types`, are moved to the repository https://github.com/opst/knitfab-api-types as [golang module](https://pkg.go.dev/github.com/opst/knitfab-api-types).

You can `go get opst/knitfab-api-types` and use it to build your Knitfab extensions.

## License

There are no changes in License.

## How to Try

### Knitfab System

Download the installer from branch [develop/v1.3.0](https://github.com/opst/knitfab/tree/develop/v1.3.0), and run

```
BRANCH=develop/v1.3.1 CHART_VERSION=v1.3.1-beta installer.sh --install
```

in the directory where you have installed Knitfab.

### CLI `knit`

Download from the assets of this release.

v1.3.0
===========

- Date: 2024-09-13

The v1.3.0 release includes a new API, "Custom Data Import", and several bug fixes.

## Important Changes

### New Feature

"Custom Data Import" API, aimed at advanced users, has been added.

With the new API, you can create a custom process for importing Data using Kubernetes PV/PVC.

For more details, see the documentation at docs/03.admin-guide/admin-guide-deep-dive.

### Bug Fix

- installer:
    - `installer.sh --prepare` appended `./knitfab-install-settings/values/knit-db-postgres.yaml`.
        - This issue has been fixed to update the entire YAML file.
    - Fix uninstaller generated by installer.
    - A typo in installer messages has been fixed.
- dataagt, run worker: Pods were not terminated if they failed to start before scheduled (for example: Volume could not be mounted).
    - Knitfab detects if scheduled Pods that become "stuck", and marks the task as "failed".
- database: During an upgrade, it was possible that both the new and old databases would run simultaneously. If it occurs, database's files could become corrupted, preventing it from starting up normally.
    - With this change, the new database is started after stopping the old one.

### Miscellaneous

- The Go version used to build Knitfab has been upgraded to v1.23.1.
- The Admin Guide has been split into two parts, "installation" and "deep-dive".
    - The "installation" part explains how to install Knitfab.
    - The "deep-dive" part provides operational guides and advanced topics.

## License

Knitfab v1.3.0 is released under BSL 1.1, as written in the LICENSE file.

CHANGE DATE for `v1.3.x` is 2028-09-13.

Previous releases, `v1.2.x` or brefore, are not changed in their CHANGE DATE.

## Upgrade Path

### Knitfab System

Download the installer, and run

```
installer.sh --install
```

in the directory where you have installed Knitfab.

### CLI `knit`

Download from assets of this release.

v1.3.0-beta
===========

- Date: 2024-09-11

The v1.3.0-beta release includes a new API, "Custom Data Import", and several bug fixes.

## Important Changes

### New Feature

"Custom Data Import" API, aimed at advanced users, has been added.

With the new API, you can create a custom process for importing Data using Kubernetes PV/PVC.

For more details, see the documentation at docs/03.admin-guide/admin-guide-deep-dive.

### Bug Fix

- installer: `installer.sh --prepare` appended `./knitfab-install-settings/values/knit-db-postgres.yaml`.
    - This issue has been fixed to update the entire YAML file.
    - A typo in installer messages has been fixed.
- dataagt, run worker: Pods were not terminated if they failed to start before scheduled (for example: Volume could not be mounted).
    - Knitfab detects if scheduled Pods that become "stuck", and marks the task as "failed".
- database: During an upgrade, it was possible that both the new and old databases would run simultaneously. If it occurs, database's files could become corrupted, preventing it from starting up normally.
    - With this change, the new database is started after stopping the old one.

### Miscellaneous

- The Go version used to build Knitfab has been upgraded to v1.23.1.
- The Admin Guide has been split into two parts, "installation" and "deep-dive".
    - The "installation" part explains how to install Knitfab.
    - The "deep-dive" part provides operational guides and advanced topics.

## How to Try

### Knitfab System

Download the installer from branch [develop/v1.3.0](https://github.com/opst/knitfab/tree/develop/v1.3.0), and run

```
BRANCH=develop/v1.3.0 CHART_VERSION=v1.3.0-beta installer.sh --install
```

in the directory where you have installed Knitfab.

### CLI `knit`

Download from assets of this release.


v1.2.1
===========

- Date: 2024-08-05

Release v1.2.1 as a **security update** .

## Important Change

Before this update, Knitfab depended on `github.com/docker/docker v25.0.3+incompatible`. The module is affected by a vulnability reported in the https://www.docker.com/blog/docker-security-advisory-docker-engine-authz-plugin/ .

Althogh Knitfab does not use authz feature, we update Knitfab to ensure security.

We have upgraded dependencies, adn as a result, `github.com/docker/docker` is removed from dependencies.

## Feature Changes, License Changes

None.

## Upgrade Path

### Knitfab System

Download the latest installer, and run `installer.sh --install` in the directory where you installed Knitfab.

### CLI `knit`

Download from assets of this release.

v1.2.1-beta
============

- Date: 2024-08-02

Pre Release v1.2.1-beta as a security update.

This is beta version, and it is not stable release.

## Important Change

Before this update, Knitfab depended on `github.com/docker/docker v25.0.3+incompatible`. The module is affected by a vulnability reported in the https://www.docker.com/blog/docker-security-advisory-docker-engine-authz-plugin/ .

Althogh Knitfab does not use authz feature, we update Knitfab to ensure security.

We have upgraded dependencies, adn as a result, `github.com/docker/docker` is removed from dependencies.

## Feature Changes, License Changes

None.

## How to Try

### Knitfab System

Download the installer from branch [develop/v1.2.1](https://github.com/opst/knitfab/tree/develop/v1.2.1), and run

```
BRANCH=develop/v1.2.1 CHART_VERSION=v1.2.1-beta installer.sh --install
```

in the directory where you have installed Knitfab.

### CLI `knit`

Download from assets of this release.

v1.2.0
=======

- Date: 2024-07-09

Release v1.2.0 as preparation for the future releases.
This release also includes a **security update** .

## Important Change

### Pod "vex" is opt-in

https://github.com/opst/knitfab/issues/91

Before this release, we emploied pods called "vex", a volume expandar.
Vex watches usage of Persistent Volumes (of Kubernetes) and expands them if needed.

But, our standard deployment depends on NFS, and
Persistent Volume based NFS does not need to be expanded. They can be written to unless NFS becomes full, regardless of the capacity of the PV.

Therefore, vex is not effective. We omitted it in standard installeation. Your computing resources are used more efficiently.

### TLS can be opted out

https://github.com/opst/knitfab/issues/92

Sometimes, Knitfab Web API does not need to be HTTPS. For example, when a Load Balancer performing TLS Termination is employed in front of Knitfab + Image Registry, Knitfab itself can be plain HTTP Web API.

So, with this release, enabling TLS can be opted out.

For more details, see `docs/03.admin-guide` .

## Bug Fixes

### `knit data push` could not be inturrupted during sending a file

https://github.com/opst/knitfab/issues/104

`knit data push` ignored Ctrl+C signal during sendin a file. Fixed.

## Security Update

Knitfab `< v1.2.0` is affected by CVE-2024-29018.

We resolved it by updating dependencies.

## Internal Change

### Schema Upgrader

https://github.com/opst/knitfab/pull/112

The Schema Upgrader Job is introduced. Schema Upgrader maintains that tables and types in RDB are up tp date.

It supports schema changing in the future.

## License

Knitfab v1.2.0 is released under BSL 1.1, as written in the LICENSE file.

CHANGE DATE for `v1.2.x` is 2028-07-09.

Previous releases, `v1.1.x` or brefore, are not changed in their CHANGE DATE.

## Upgrade Path

Get the latest installer, and run `installer.sh --install` in the directory where you installed Knitfab.

v1.1.2
======

- Date: 2024-06-24

Release v1.1.2 as re-release of v1.1.1.

This release retracts v1.1.1.

## Changes

Nothing, but Charts and Images which v1.1.1 have missed are published.

v1.1.1
======

- Date: 2024-06-11
- Retracted: 2024-06-24

Release v1.1.1, as a security update.

> [!Important]
>
> This release has a probrem to install.
>
> We have not deploy charts and images for this version, so installer fails to install v1.1.1.
>
> Sorry for inconvinient. Please use v1.1.2. There are no change of features.

## Affected Users

No. Only developer are affected by this security issue.

We using Ansibel as the provisioner for dev-cluser. This release upgrade ansible and solve vulnabilites in old ansible.

## New Feature

Nothing.

## Upgrade Path

Nothing. This release has no change in Knitfab CLI, Containers nor Installer.

Knitfab binaries are independent from these vulnabilities comes from old ansible.

To all users, use v1.1.0 for CLI and admin-tools. Installer and Container Images are not changed from v1.1.0 .

v1.1.0
======

- Date: 2024-06-07

This is the our first "minor update" release.
We introduce some new features, and update CHANGE DATE in LISENCE file.

Important Change
-----------------

### New Features

#### Cross Build

https://github.com/opst/knitfab/issues/48

Knitfab Contaienr Images supports ARM in addintion to AMD(x86_64).

### Timebased Query

https://github.com/opst/knitfab/issues/19 and https://github.com/opst/knitfab/issues/22

`knit data find` and `knit run find` can filter Data or Run by time. To do that, use uew flags, `--sicne` and `--duration`.

For more detail, see `knit data find --help` or `knit run find --help`

### Extentions

https://github.com/opst/knitfab/issues/17

Backup and Restore tools are provided.
This toolkit supports backing up Knitfab Data, Run and Plans. You can download them from this release page.

This toolkit backs up by taking a dump of RDB and copies of Persistent Volume and its Claim.

https://github.com/opst/knitfab/issues/51

Knitfab supports Web Hooks, for users who want to extend Knitfab.

By this release, Knitfab provides "Lifecycle Hooks", which are triggerd when the status of a Run is updated. Subscriber of these Lifecycle Hooks can receive Run infomation before and after chaning status.

https://github.com/opst/knitfab/issues/52

Knitfab supports Extra Web API. Knitfab Web API Server, `knitd`, proxies requests sent to pathes to corresponding URL as written in configuration file.

For more details of 3 features above, read `docs/03.admin-guide`.

https://github.com/opst/knitfab/issues/53

Knitfab CLI `knit` supports Extension Command or custom command.

`knit foo` finds command `knit-foo`, `knit-foo.exe`, `knit-foo.cmd`, `knit-foo.bat` or `knit-foo.com`, and then, invoke it with all args and flags. STDIN, STDOUT and STDERR are also passed to Extension Command.

It let know Knitfab's configuration via environmental variables, `KNIT_PROFILE`, `KNIT_PROFILE_STORE`, `KNIT_ENV`.

For more detail, see `docs/02.user-guide`.

CHANGE DATE of our license
---------------------------

This release, v1.1.0, is made at 2024-06-07.

So, CHANGE DATE *for v1.1.x* is 2028-06-07, 4 years later from now. This CHANGE DATE will not changed by bugfix releases.

CHANGE DATE *for v1.0.x* is not changed.

Upgrade Path
-------------

Download new installer script and run `./installer.sh --install` in the directory where you installed Knitfab.

v1.0.3
=======

- Date: 2024-05-07

This is a “bug fix” release. No changes in features.
By this release, Knitfab works more stable. It contains security update.

Important Change
-----------------

### Bug Fixes

- https://github.com/opst/knitfab/issues/58
    - Resolve the problem that `knit data pull` fails randomly.
- https://github.com/opst/knitfab/issues/64
    - Knitfab could execute Runs which are equivalent with existing Runs, duplicately. It could occur when a Data which is one of inputs of a Run is removed from and put on again Tags (`knit data tag --remove …` and then `knit data tag --add …`).

### Security

Because of https://pkg.go.dev/vuln/GO-2024-2687 , dependencies are upgraded.

Also Go is updated to 1.22.2 .

### Lisence

Knitfab v1.0.3 is released under BSL v1.1 (Business Source License) as before.
By this release, Licensed Work targets a Minor Version. LICENSE document is updated.

- Before this release, Licensed Work targets Knitfab v1.0.0 only.
- By this release, Licensed Work targets Knitfab v1.0, meaning “any versions from v1.0.0 before v1.1.0” (v1.1.0 and later is out of target).

The Change Date of the License stays 2028-04-01, after this release until v1.1.0 is released.

How to Upgrade
---------------

By this release, knitfab installer performs an upgrade when it detects that Knitfab has been installed.
To upgrade, run the new installer as like `installer.sh --install …` with the settings directory created when you installed Knitfab.

Detailed upgrade steps are following.

**Requiremnt** :

The settings directory created when you installed Knitfab. It is created by `installer.sh --prepare …`, and it is named `knitfab-install-settings` by default.

**Upgrading Steps**:

Upgrade your installer.

```
wget -O installer.sh https://raw.githubusercontent.com/opst/knitfab/main/installer/installer.sh
chmod +x ./installer.sh
```

Re-install Knitfab.

```
./installer.sh --install -n NAMESPACE -s knitfab-install-settings
```

It is not necessary to run `./installer.sh --prepare …`.

The file path `knitfab-install-settings` should be the your settings directory.
`NAMESPACE` should be the Namespace of Kubernetes where Knitfab to be upgraded is installed.

It uses the kubeconfig file in the settings directory by default. If you need to specify another kubeconfig, pass it by the environmental variable `KUBECONFIG` or `--kubeconfig` flag.

Detailed usage of `./installer.sh` will be shown by running `./installer.sh` without any arguments.

It is noted that the value of `NAMESPACE` is saved as a default Namespace in the settings directory, by this upgrading. After this, the installer uses the value as Namespace if `NAMESPACE` is not passed.


v1.0.0
=========

- Date: 2024-04-01

New Feature & Feature Update
----------------------------

- Initial Release.
