#!/usr/bin/env bash

cd ../..
cd Agile.Wasp2/
#git pull
sbt clean publishLocal

cd ../Agile.Wasp2.Whitelabel/docker