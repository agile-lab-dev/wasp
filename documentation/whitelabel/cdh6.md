# CDH6 Whitelabel Project Structure

Wasp is deployed on CDH6 as an docker environment. 

The project structure is the following:
```
whitelabel
├─── docker
...     ├─── cdh5
        ├─── ...
        ├─── cdh6
               ├─── cdh-docker
               │       ├─── hbase
               │       ├─── hdfs
               │       ├─── kafka
               │       ├─── mongodb
               │       ├─── schema-registry
               │       ├─── solr
               │       ├─── spark
               │       ├─── spark2
               │    build-and-push.sh
               │    cloudera.list
               │    cloudera.pref
               │    Dockerfile
               │    entrypoint.sh
               │    resolve-templates.sh
               ├─── templates
               │    hbase-site.xml
               │
               .lab.yml
               docker-entrypoint.sh
               docker-environment.conf
               get-docker-cmd.sh
               log4j2.properties
               start-wasp.sh
               supervisord.conf
               start-wasp-one.sh
               docker-entrypoint-one.sh
```

### cdh-docker
In the cdh-docker directory is the configuration and build of an base image that will later host wasp. 
It consists of the service templates, a build-and-push.sh script which publishes the docker image to the registry and a Dockerfile with an entrypoint script.

The images used for building the services are Ubuntu 16.04 and Nifi.

### Bash scripts
start-wasp.sh and start-wasp-one.sh are the two main bash scripts that start WASP; start-wasp.sh will start all containers. Using the additional option -d, WASP will start with a clean configuration. The scripts start a docker container with an entrypoint(either docker-entrypoint-one.sh or docker-entrypoint.sh). docker-environment.conf holds all the configurations for the services.

