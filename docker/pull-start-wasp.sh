#!/usr/bin/env bash

cd ..
git pull
./docker/start-wasp.sh

wait