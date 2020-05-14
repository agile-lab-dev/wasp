#!/usr/bin/env bash

set -ax

docker login registry.gitlab.com/agilefactory/agile.wasp2

docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:6.3.2
docker tag registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:6.3.2 registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:latest

docker push registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:6.3.2
docker push registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:latest