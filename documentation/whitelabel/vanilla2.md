# Vanilla Hadoop 2.x Whitelabel Project Structure

Wasp is deployed on Hadoop "vanilla" as an docker environment.

The project structure is the following:
```
whitelabel/docker/vanilla-hadoop2/
├── build-and-tag.sh
├── docker
│   ├── Dockerfile
│   ├── install-scripts
│   │   ├── common
│   │   │   ├── apt-clean-caches.sh
│   │   │   └── bash-defaults.sh
│   │   ├── configure-hadoop.sh
│   │   ├── configure-spark.sh
│   │   ├── install-hadoop.sh
│   │   ├── install-hbase.sh
│   │   ├── install-kafka.sh
│   │   ├── install-mongo.sh
│   │   ├── install-prerequisites.sh
│   │   ├── install-solr.sh
│   │   └── install-spark.sh
│   ├── runtime
│   │   └── entrypoint.sh
│   └── templates
│       ├── hadoop
│       │   ├── core-site.xml
│       │   ├── mapred-site.xml
│       │   └── yarn-site.xml
│       ├── hbase
│       │   ├── hbase-site.xml
│       │   └── regionservers
│       ├── kafka
│       │   └── server.properties
│       ├── mongo
│       │   └── mongod.conf
│       ├── resolve-templates.sh
│       └── solr
│           └── solr.in.sh
├── docker-entrypoint-one.sh
├── docker-entrypoint.sh
├── docker-environment.conf
├── docker-environment_reference.conf
├── get-docker-cmd.sh
├── log4j-consumer.properties
├── log4j-master.properties
├── log4j-producer.properties
├── log4j-single.properties
├── start-wasp-one.sh
├── start-wasp.sh
└── supervisord.conf
```

### docker

In the docker directory you can find the configuration and build of a base image that will later host wasp.

It consists of the service templates, a build-and-tag.sh script which builds and tag the docker image and a Dockerfile with an entrypoint script.

The images used for building the services are Ubuntu 16.04 and Nifi.

### Bash scripts
start-wasp.sh and start-wasp-one.sh are the two main bash scripts that start WASP; start-wasp.sh will start all containers. Using the additional option -d, WASP will start with a clean configuration. The scripts start a docker container with an entrypoint(either docker-entrypoint-one.sh or docker-entrypoint.sh). docker-environment.conf holds all the configurations for the services.

