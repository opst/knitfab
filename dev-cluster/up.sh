#! /bin/bash
cd ${0%/*}

poetry run vagrant up $@

./install-cni.sh
./install-registry.sh
