#!/usr/bin/env bash

set -ax

cd base
cd cache
wget https://archive.cloudera.com/cdh7/7.1.7.0/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976-el7.parcel
mv CDH-7.1.7-1.cdh7.1.7.p0.15945976-el7.parcel CDH-7.1.7-1.cdh7.1.7.p0.15945976-el7.parcel.tar.gz
cd ..
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdp:base
cd ..
cd worker
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdp:worker

docker login registry.gitlab.com/agilefactory/agile.wasp2
docker push registry.gitlab.com/agilefactory/agile.wasp2/cdp:base
docker push registry.gitlab.com/agilefactory/agile.wasp2/cdp:worker
