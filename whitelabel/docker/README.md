# Docker for WASP

This folder contains scripts, dockerfiles, Docker Compose files and application configurations that enable you to run both WASP and the services it depends on in Docker containers.

This is the intended way to use and test WASP during development.

## Setup

Both functionalities (WASP containers and services containers) rely on Docker images which need to be built.

For WASP containers you'll need the `agilefactory/oracle-java` image, which can be built as follows:
- enter the `oracle-java` subdirectory
- launch `build_oracle_java.sh`

For service containers you'll need the `base_cdh` image, which can be built as follows:
- enter the `base_cdh` subdirectory
- launch `build_base_cdh.sh`

## Running WASP

Launch `start-wasp.sh`.

This runs the various processes used by WASP in docker containers. See the command line help for more information.

**For the whitelabel project:** use `start-whitelabel-wasp.sh` to start the whitelabel project.

## Running the services

Launch `start-services.sh service1 service2 ...` where `service1` wtc are the names of the services to start.

This runs the various services supported/needed by WASP in docker containers. See the command line help for more information.

**For the whitelabel project:** use `start-whitelabel-services.sh` to start the services needed for the whitelabel.