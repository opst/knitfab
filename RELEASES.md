v.1.2.1
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
