#!/usr/bin/env bash

set -ax

cd docker
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/wasp-hadoop-vanilla-2.12:2

#docker login registry.gitlab.com/agilefactory/agile.wasp2
#docker push registry.gitlab.com/agilefactory/agile.wasp2/cdh7:base
#docker push registry.gitlab.com/agilefactory/agile.wasp2/cdh7:worker
