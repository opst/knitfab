knitfab/database [postgres]
===========================

This chart install knitfab database with PostgreSQL.

Values
-------

- `component`: (optional) component name. It is also Service name exposing PostgreSQL.
    - Default: `database`
- `initialCapacity`: (optional) initial capacity of PV containing data directory of PostgreSQL.
    - Default: 1GiB
- `port`: (optional) service port where PostgreSQL listening
    - Default: 5432
- `credential.name`: (optional) secrert name which will contain credential to PostgreSQL.
    - Default: `"database-credential"`
- `credential.username`: (optional) username for database
    - Default: `"knit"`
- `credential.password`: **(MANDATORY)** password for database
- `storage.system`: **(MANDATORY)** StorageClass name for [data directory](https://www.postgresql.org/docs/13/runtime-config-file-locations.html#GUC-DATA-DIRECTORY)

Objects to be created
----------------------

- **service** exposing PostgreSQL: named as `{{ .Values.component }}`
- **secret** containing credential to PosgresSQL: named as `{{ .Values.credential.name }}`
- **deployment** running PostgreSQL: with database named "knit".
