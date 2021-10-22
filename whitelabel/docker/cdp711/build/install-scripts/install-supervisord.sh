#!/usr/bin/env bash

source ./common/bash-defaults.sh

yum install -y supervisor

exec bash ./common/yum-clean-caches.sh 
