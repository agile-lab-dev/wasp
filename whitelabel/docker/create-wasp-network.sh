#!/usr/bin/env bash

echo "Setting up docker network \"wasp-docker\""
source get-docker-cmd.sh


# check if network exists
if [ $($DOCKER_CMD network ls | grep "wasp-docker" | wc -l) -eq 0 ]; then
    # network not found, create
    echo "Docker network \"wasp-docker\" does not exist; creating it..."
    $DOCKER_CMD network create wasp-docker
else
    # network found
    echo "Docker network \"wasp-docker\" already exists"
fi