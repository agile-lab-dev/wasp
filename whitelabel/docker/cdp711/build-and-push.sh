#!/usr/bin/env bash

set -ax

cd build
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdp:717

docker login registry.gitlab.com/agilefactory/agile.wasp2
docker push registry.gitlab.com/agilefactory/agile.wasp2/cdp:717
