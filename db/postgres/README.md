database initialize scripts for postgres
=========================================

This directory is organized as...

- `./init` : scripts which should be run on initializing.
    - `./0-ddl` : `create` something. (mostly, `create table ...`)
        - Files in the directory should have name start with `0`.
        - This is symlink of `../../helm/knit-test/assets/pkg/db/postgres` .
        - To share table/types definition between production and test
            - Helm do not allow for a chart to access outside from itself,
            ddl for testing should be in testing chart.
            - Production chart is composed with build script. The build script can copy files, even if these are in test charts.
    - `./1-seed` : `insert` something to provide initial data.
        - Test environment have no interest in this.
        - Files in the directory should have name starts with `1`.

Naming convention for files are need to fix execution order.
See https://hub.docker.com/_/postgres ("Initialization scripts") for more details.

These files are copied to our installer package by build script (`build/build.sh`) .
