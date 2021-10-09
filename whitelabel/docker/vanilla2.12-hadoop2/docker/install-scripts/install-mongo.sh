#!/usr/bin/env bash
source ./common/bash-defaults.sh

wget --progress=bar:force:noscroll --no-check-certificate https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org-mongos_3.4.8_amd64.deb
wget --progress=bar:force:noscroll --no-check-certificate https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org-server_3.4.8_amd64.deb
wget --progress=bar:force:noscroll --no-check-certificate https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org-shell_3.4.8_amd64.deb
wget --progress=bar:force:noscroll --no-check-certificate https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org-tools_3.4.8_amd64.deb
wget --progress=bar:force:noscroll --no-check-certificate https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org_3.4.8_amd64.deb
dpkg --force-confold -i mongo*.deb
rm mongo*.deb

