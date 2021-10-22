#!/usr/bin/env bash

source ./common/bash-defaults.sh

yum update -y
yum install -y mongodb-org

exec bash ./common/yum-clean-caches.sh 
