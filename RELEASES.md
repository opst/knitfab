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
