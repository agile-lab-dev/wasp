# CDH7 Whitelabel Project Structure

Wasp is deployed on CDH6 as an docker environment. 

The project structure is the following:
```
whitelabel
├─── docker
...     ├─── cdh5
        ├─── ...
        ├─── cdh7
               ├─── base
               │       ├─── cache
               │       ├─── services/ssh
               │    Dockerfile
               ├─── worker
               │       ├─── scripts
               │       │     ├─── hbase
               │       │     ├─── hdfs
               │       │     ├─── kafka
               │       │     ├─── mongodb
               │       │     ├─── yarn               
               │       │     ├─── solr
               │       │     ├─── zookeeper
               │       │    start-all.sh
               │       │─── templates
               │       │     ├─── hbase
               │       │     ├─── hdfs
               │       │     ├─── kafka
               │       │     ├─── mongo
               │       │     ├─── spark          
               │       │    hbase-site.xml
               │     docker-compose.yml
               │     Dockerfile
               │     resolve-templates.sh
               │
            build-and-push.sh
            docker-entrypoint.sh
            docker-environment.conf
            get-docker-cmd.sh
            log4j2.properties
            start-wasp.sh
            supervisord.conf
```

### base
In the **base** directory is the configuration and build of an base image that will later host wasp. During the build it downloads the newest parcels from the cloudera repo.

The images used for building the services are centos/systemd.

Currently the repo that is being used was closed by cloudera. The updated repo has some issues with kafka. WORK IN PROGRESS

### worker
In the **worker** directory is then using the base image to install the downloaded parcels, and start the services needed for wasp.

## Usage
- use the build-and-push.sh to publish the docker images to the registry.
- start the start-wasp.sh script.

### Notes
- For some reason the kafka server will not start on this docker environment. Wasp is deployed(with some warnings) but cannot start a pipegraph or producers, because it cannot create a topic. 

```
[producers] ERROR 2021-08-12 13:04:00,188 i.a.b.w.c.k.KafkaAdminActor: Error in topic 'test_avro.topic' creation
org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 1 larger than available brokers: 0.
[producers] INFO  2021-08-12 13:04:00,195 i.a.b.w.c.k.KafkaAdminActor: CheckOrCreateTopic(test_avro.topic,3,1): false

```