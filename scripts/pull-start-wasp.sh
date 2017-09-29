#!/usr/bin/env bash

cd ..
git pull
cd docker
./start-wasp.sh

wait