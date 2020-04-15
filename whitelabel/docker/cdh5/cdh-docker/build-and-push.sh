#!/usr/bin/env bash

set -ax

docker login registry.gitlab.com/agilefactory/agile.wasp2

docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:5.16
docker tag registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:5.16 registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:latest

docker push registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:5.16
docker push registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:latest