database initialize scripts for postgres
=========================================

This directory is organized as...

- `./schema/versions/` : DDLs and Data Seeders splitted into versions.
    - `1/*.sql`: for Schema Version 1.

For each version, SQL Scripts are applied to the Database in lexical order.

These files are copied to our installer package by build script (`build/build.sh` and `build/lib/image.sh`) .
