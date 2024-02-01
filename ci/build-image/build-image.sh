#!/bin/bash
NAME='registry.gitlab.com/agilefactory/agile.wasp2/sbt:1.9.8-8u402-b06-jdk-jammy'
docker buildx build --platform linux/amd64 \
             --build-arg BASE_IMAGE_TAG=8u402-b06-jdk-jammy \
             --build-arg SBT_VERSION=1.9.8 \
             --build-arg SCALA_VERSION=2.11.12 \
             . -t $NAME

docker push $NAME