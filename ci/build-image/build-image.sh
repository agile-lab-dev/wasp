#!/bin/bash
SBT_VERSION=1.10.7
BASE_IMAGE_TAG=17.0.13_11-jdk-jammy
NAME="registry.gitlab.com/agilefactory/agile.wasp2/sbt:${SBT_VERSION}-${BASE_IMAGE_TAG}"
docker buildx build --push --platform linux/amd64,linux/arm64 \
             --build-arg BASE_IMAGE_TAG=${BASE_IMAGE_TAG} \
             --build-arg SBT_VERSION=${SBT_VERSION} \
             --build-arg SCALA_VERSION=2.12.10 \
             . -t "${NAME}"