#!/usr/bin/env bash

source ./common/bash-defaults.sh

yum install epel-release -y
yum update -y
yum install -y java-1.8.0-openjdk-headless

exec bash ./common/yum-clean-caches.sh 
