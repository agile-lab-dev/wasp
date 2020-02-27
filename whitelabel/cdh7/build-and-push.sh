#!/usr/bin/env bash

set -ax

cd base
cd cache
wget https://archive.cloudera.com/cdh7/7.0.3.0/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019-el7.parcel
mv CDH-7.0.3-1.cdh7.0.3.p0.1635019-el7.parcel CDH-7.0.3-1.cdh7.0.3.p0.1635019-el7.parcel.tar.gz
cd ..
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdh7:base
cd ..
cd worker
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdh7:worker

docker login registry.gitlab.com/agilefactory/agile.wasp2
docker push registry.gitlab.com/agilefactory/agile.wasp2/cdh7:base
docker push registry.gitlab.com/agilefactory/agile.wasp2/cdh7:worker
