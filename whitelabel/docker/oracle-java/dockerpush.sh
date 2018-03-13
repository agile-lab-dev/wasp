#!/usr/bin/env bash

REPOSITORY_USERNAME=
REPOSITORY_PASSWORD=
REPOSITORY_URI=

docker login -u ${REPOSITORY_USERNAME} -p ${REPOSITORY_PASSWORD} ${REPOSITORY_URI}
docker push ${REPOSITORY_URI}/agilefactory/oracle-java:jdk-8
docker push ${REPOSITORY_URI}/agilefactory/oracle-java:jdk-8u162