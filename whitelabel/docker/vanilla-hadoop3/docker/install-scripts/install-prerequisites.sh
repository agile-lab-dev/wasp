#!/usr/bin/env bash

source ./common/bash-defaults.sh

apt-get update
apt-get install -y apt-transport-https
apt-get install -y ca-certificates wget sudo
apt-get install -y openjdk-8-jdk-headless software-properties-common #hadoop prerequisites
apt-get install -y gettext-base 
apt-get install -y multitail
apt-get install -y supervisor
apt-get install -y awscli

apt-get update
apt-get install -y python3
apt-get install -y python3-flask python3-boto3

exec bash ./common/apt-clean-caches.sh 