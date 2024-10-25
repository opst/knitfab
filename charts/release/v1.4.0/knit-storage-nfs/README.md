knitfab/storage [nfs]
=========================

This chart defines `StorageClass` for knitfab, provisioning with [csi-driver-nfs](https://github.com/kubernetes-csi/csi-driver-nfs).

Prerequirement
---------------------

- **YOUR OWN** nfs server.
- helm

Values
------

- `.component`: (optional) component name.
    - Default: `knitfab-storage`
- `.data` : (optional) Names of `StorageClass` for kntifab "data".
    - Default: `knitfab-storage-data`
- `.system` : (optional) Names of `StorageClass` for others (rdb, image registry...).
    - Default: `knitfab-storage-system`
- `.nfs` : **(MANDATORY)** your NFS connection configuration.
    - `server`: **(MANDATORY)** hostname or IP of your nfsd.
    - `exportRoot`: (optional) export root of your nfsd.
        - Default: `/`
    - `mountOptions`: (optional) NFS mount options, in comma-separated string


Objects to be installed
------------------------

- Storage Classes with named `{{ .data }}` and `{{ .system }}`
- Secret named `{{ .component }}-mount-option`
    - containing BFS mount option. For more detail, see https://github.com/kubernetes-csi/csi-driver-nfs/blob/master/docs/driver-parameters.md#provide-mountoptions-for-deletevolume
