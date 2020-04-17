# WASP ChangeLog

## WASP 2.0.5
25/01/2018

- Miglioramento complessivo error/exception-handling dai seguenti punti di vista:
	- log in console dei vari componenti
	- propagazione e gestione errori - ora riportati fino alla REST-response
	
	N.B. il timeout passato alla `WaspSystem.??()` (se non esplicitato viene usato general-timeout-millis di *.conf) è ora inteso come "tempo complessivo per gestire la REST" e non più come "tempo per svolgere la specifica operazione interna (es. avvio pipegraph)": a partire da general-timeout, lo slot di tempo assegnato ai livelli inferiori di annidamento è via via ridotto di 5sec

- Gestione down(post-unreachable) / reJoin(post-reachable) dei membri XyzMasterGuardian del cluster Akka - gestito tramite ClusterListenerActor (e `actor-downing-timeout-millis` di *.conf)
	
	N.B. almeno un seed-node deve rimane in vita per poter fare reJoin!!!

- Gestione launcher tramite CommanLine invece che lista argomenti


## WASP 2.1.0
06/02/2018

**Fix**
- A seguito di failure di StartPipegraph, SparkConsumersMasterGuardian e RtConsumersMasterGuardian non rimangono più in stato `starting` ma ritornano in `unitialized`, evitando quindi lo stash() di RestartConsumers durante StartPipegraph successivi

- Corretto uso di log4j direttamente da WASP framework

**Update**
- Miglioramento complessivo error/exception-handling durante StopProducer e StopPipegraph

- Log di stackTrace al posto del solo message da parte dell'attore che gestisce l'eccezione (continua ad esser propagato il solo message)

- Allineata cartella docker (yml e sh) per futuro uso WhiteLabel

- Solr unique key: IndexModel accetta parametro opzionale `idField` per indicare quale campo usare come id al posto di autogenerare UUID random

- Elastic Spark upgrade to 6.1 for Structured Streaming

	yml di riferimento rimane `docker/elastickibana-docker-compose.yml`

- Gestione parametri commandLine e relativo allineamento `docker/start-wasp.sh`
	
	Parametri disponibili: -h (help), -v (versione), -d (MongoDB dropping)
		
		-h, -v	ricevuto da tutti
		-d	ricevuto solo da master

- Aggiornamento di `reference.conf`: WASP usa ora i default presi da `reference.conf`; `docker/docker-environment.conf` funge per ora come `template-whitelabel` dove sono presenti le KEY da sovrascrivere obbligatorie e invece commentate tutte le altre KEY possibili
	
	N.B. per mantenere scalabile la soluzione, i VALUE di default presenti in `reference.conf` non sono anche riportati in `docker/docker-environment.conf`


## WASP 2.1.1
07/02/2018

**Fix**
- GitLab CI to compile and publish on internal (nexus) and public (criticalcase)


## WASP 2.1.2
12/02/2018

**Fix**
- GitLab CI rimossa da master
- Scommentate le KEY `driver-hostname` della `template-whitelabel` `docker/docker-environment.conf` di `spark-streaming` e `spark-batch`

**Update**
- HBASE Writer - gestione celle create dinamicamente in stile Cassandra


## WASP 2.1.3
16/02/2018

**Update**
- MongoDB fullwriteConsistency


## WASP 2.2.0
16/02/2018

**Fix**
- Corretta la KEY `driver-hostname` della `template-whitelabel` `docker/docker-environment.conf` di `spark-batch`
- MongoDB, Elastic, Solr considerano ora il timeout di configuration espresso in millis

**Update**
- WaspRELEASE_NOTES + WhiteLabelREADME

- Trait `Strategy` estende `Serializable`

- Revisione connectionTimeout verso MongoDB, Solr, Elastic

- Impostazione WhiteLabel (per usarla: `whitelabel/docker/start-whitelabel-wasp.sh`)

- Riportata in WaspSystem la creazione della collection MongoDB `configurations` (in modo venga eseguita da tutti i container nello stesso modo)

- Revisione della gestione `dropDB` tramite commandlineOption `-d` di `start_wasp.sh`: Master fa solo drop ed esce (senza reinizializzare)

- Modulo consolePlugin separato da modulo consumers-spark

- Migrazione totale cross-reference da byId a byName delle collection MongoDB


## WASP 2.3.0
26/02/2018

**Fix**
- Corrette dipendenze ElasticSearch

**Update**
- Consistenza/atomicità su waspDB.insertIfNotExist: permette che non avvengano scritture contemporanee/duplicate

- Supporto Solr per nested document

- Whitelabel manual/auto-test per Console, Solr, HDFS, ElasticSearch

- LoggerPipegraph su Solr tramite StructuredStreaming

- Aggiunto service `banana` (Data visualization plugin per Solr) su porta 32770 con template di default per `logger_index_shard1_replica1`: `whitelabel/docker/solrcloud-docker-compose.yml` -> `whitelabel/docker/solrcloudbanana-docker-compose.yml`

- Revisione uso additionalJars

- Batch separato da streaming (container apposito) ma in stesso modulo consumers-spark


## WASP 2.4.0
02/03/2018

**Fix**
- Solr JsonSchema: rollback a gestione "estrapola/invia a Solr solo il contenuto del campo `properties`" (gestione ad-hoc rispetto ElasticSearch)

**Update**
- Aggiornamento di `reference.conf` e `whitelabel/docker/docker-environment.conf`
	- `spark-streaming` e `spark-batch`: riordinate le KEY, aggiunta KEY `driver-conf` che incapsula le configurazioni relative al driver (nuovo `submit-deploy-mode` con default "client"), aggiunta KEY `retained-jobs` (default 100)
	- `solrcloud`: rimossa KEY `cluster_name`
	- `elastic`: rimossa KEY `cluster-name`

- Solr: uso di `zookeeperConnections` al posto di `apiEndPoint`

- Revisione gestione batchJobs: avvio parallelo di istanze di batchJobs diversi (vedi nuova collection MongoDB batchjobinstances)


## WASP 2.5.0
09/03/2018

### Update
- Modificato `chroot_path` di zookeeper per Kafka: da `""` a `"/kafka"` (allineamento con Solr per cui è `"/solr"`)

- Aggiunto JDBCReader plugin

- Aggiunto uso di kryoSerializator tramite `kryo-serializer` in`reference.conf` e `whitelabel/docker/docker-environment.conf` (default: `enabled = true, registrators = "", strict = false`)

- Rimozione `broadcast()` nei seguenti punti:
    - `strategy` in `LegacyStreamingETLActor` / `StructuredStreamingETLActor`
    - `topic.getDataType` in `KafkaReader`

- Aggiunto IndexModelBuilder per gestire Solr/Elastic in modo distinto

### Resolve "Release note generator tool"

[Merge request 22](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/22)

Updated at: 2018-03-09T15:03:26.784Z

Branch: feature/85-release-note-generator-tool

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Closes #85 

```
cd tools/release-note-generator
python setup.py install
wasp-release-note-generator --token 'YOUR GITLAB AUTHENTICATION TOKEN' --sprint 'Sprint 2.5' > file.md

```

### Resolve "[plugin] jdbReader improvement"

[Merge request 20](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/20)

Updated at: 2018-03-09T14:40:28.653Z

Branch: feature/83-plugin-jdbreader-improvement

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #83

### Resolve "[kryo-config] Set new configs for Spark streaming/batch"

[Merge request 17](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/17)

Updated at: 2018-03-09T11:14:44.422Z

Branch: feature/73-kryo-config-set-new-configs-for-spark-streaming-batch

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #73

The config `kryo-serializer.strict` within `wasp.spark-streaming` / `wasp.spark-batch` are mapped to spark `spark.kryo.registrationRequired` in `Agile.Wasp2/consumers-spark/src/main/scala/it/agilelab/bigdata/wasp/consumers/spark/utils/SparkUtils.scala`)

**Note**: This config have to be "false" in order to correctly work without N class registrations (see https://spark.apache.org/docs/2.2.1/configuration.html#compression-and-serialization and https://github.com/EsotericSoftware/kryo#registration)

### Resolve "[gitlab-ci] organize branching model and deployment model"

[Merge request 18](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/18)

Updated at: 2018-03-08T16:23:37.998Z

Branch: feature/66-gitlab-ci-organize-branching-model-and-deployment-model

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Closes #66 

Branching model now follows the rules described in [BranchingModelSupport](project/BranchingModelSupport.scala)

BaseVersion(2.5.0)

* if branch name is develop -> 2.5.0-SNAPSHOT
* if branch name is release/v2.5 -> 2.5.0-SNAPSHOT
* if branch name is feature/issue-issue-text -> 2.5.0-issue-issue-text-SNAPSHOT
* if branch name is hotfix/hotfix -> 2.5.0-hotfix-SNAPSHOT
* if tag name is v2.5.0 -> 2.5.0
* if branch name is not release/v2.5 -> exception
* if tag name is not v2.5.0 -> exception


To use the branching model

```scala
import BranchingModelSupport._

val baseVersion = BaseVersion(2,5,0)

//retrieves branch name from gitlab ci environment or from current repository as fallback
version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

//retrieves branch name from current repository
version in ThisBuild := versionForContainingRepository(baseVersion)

//retrieves branch name from constant
version in ThisBuild := versionForConstant("develop")(baseVersion)

```


## WASP 2.6.0
16/03/2018

### Added Kerberos integration to WASP2 and other stuff

[Merge request 23](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/23)

Created at: 2018-03-12T09:49:27.083Z

Updated at: 2018-03-16T13:20:53.709Z

Branch: feature/20-kerberos

Author: [Mattia](https://gitlab.com/MattiaB)

Assignee: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #20, #63, #71, #54, #89

- Created a new docker image for wasp2; also changed the start-wasp script to integrate with the security
- Fixed the wasp kafka producer to write in the kerberos enviroment
- Some fix to write with Hbase and Solr in the kerberos env
- Added some documentation to run wasp2 in YARN-mode with kerberos
- Added general options for spark
- Added more test for Hbase (keyValue datastore) and KafkaWriter

### Resolve "[rest] improvements"

[Merge request 32](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/32)

Created at: 2018-03-14T17:44:03.445Z

Updated at: 2018-03-15T08:49:13.440Z

Branch: feature/97-rest-improvements

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #97 

- `/help` returns `wasp` as a JSON arrayOfObjects instead of a JSON objectOfObjects
- Added `pretty=true` optional URI param in order to receive JSON beautified

### Resolve "[improvement] master dropDB-mode issue"

[Merge request 33](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/33)

Created at: 2018-03-15T10:38:47.104Z

Updated at: 2018-03-16T13:20:57.566Z

Branch: feature/100-improvement-master-dropdb-mode-issue

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #100

In  WaspLauncher.initializeWasp(): pre-checked if the current node is a `master` and done an ad-hoc mngm


## WASP 2.7.0
26/03/2018

### Resolve "[rest] batchjob-start REST json parameter"

[Merge request 38](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/38)

Created at: 2018-03-16T18:31:17.440Z

Updated at: 2018-03-21T09:34:24.638Z

Branch: feature/104-rest-allow-batchjob-rest-post-parameters

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #104, #86 

**Usage example**

cURL

```bash
curl -X POST \
  http://localhost:2891/batchjobs/TestBatchJobFromHdfsFlatToConsole/start \
  -H 'Content-Type: application/json' \
  -d '{
        "stringKey": "aaa",
        "intKey2": 5
      }'
```

Rest API => POST http://localhost:2891/batchjobs/_batchJobName_/start

Header => Content-Type: application/json

Body => _jsonContent_

**Note**

For the resulting batchJob instance start, the json keys (in the REST json parameter) will be merged with the batchJobETL strategy configuration (Typesafe Config) keys of the related batchJob => the merged configuration is injected in the Strategy `configuration` (Typesafe Config)

*N.B. json keys (in the REST json parameter) override any duplicate keys of the specific batchJob*


### Resolve "[whitelabel] testcases"

[Merge request 39](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/39)

Created at: 2018-03-19T15:21:37.224Z

Updated at: 2018-03-21T13:44:40.817Z

Branch: feature/106-whitelabel-verify-hbase-testcase

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #106 

Fix HBase
* `docker-service-configuration/hdfs` => `docker-service-configuration/hadoop` including also `hbase-site.xml`
* `whitelabel/docker/start-wasp.sh`:

```
DOCKER_OPTS="$DOCKER_OPTS -v $SCRIPT_DIR/docker-service-configuration/hdfs:/etc/hadoop/conf/:ro ..."
```

=>

```
DOCKER_OPTS="$DOCKER_OPTS -v $SCRIPT_DIR/docker-service-configuration/hadoop:/etc/hadoop/conf/:ro ..."
```


### Resolve "[config] revise core-site.xml and hbase.site.xml in containers"

[Merge request 40](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/40)

Created at: 2018-03-21T13:36:28.900Z

Updated at: 2018-03-21T14:01:03.491Z

Branch: feature/108-config-revise-core-site-xml-and-hbase-site-xml-in-container-and

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #108
* `core-site.xml` in solr, hdfs, hbase containers:

```
hdfs://hdfs-namenode:9000
```
=>
```
hdfs://namenode:9000
```

* `whitelabel/docker/start-wasp.sh`:

```
DOCKER_OPTS="$DOCKER_OPTS -v $SCRIPT_DIR/docker-service-configuration/hdfs:/etc/hadoop/conf/:ro  -v $SCRIPT_DIR/docker-service-configuration/hbase:/etc/hbase/conf/:ro"
```

=>

```
DOCKER_OPTS="$DOCKER_OPTS -v $SCRIPT_DIR/docker-service-configuration/hadoop:/etc/hadoop/conf/:ro"
```

*  hbase in `reference.conf`:

```
    core-site-xml-path = "/etc/hbase/conf/core-site.xml"
    hbase-site-xml-path = "/etc/hbase/conf/hbase-site.xml" 
```

 =>

```
    core-site-xml-path = "/etc/hadoop/conf/core-site.xml"
    hbase-site-xml-path = "/etc/hadoop/conf/hbase-site.xml"
```

### GL-111: Producer actor now support extract partition key value

[Merge request 44](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/44)

Created at: 2018-03-23T17:29:37.763Z

Updated at: 2018-03-26T10:29:38.088Z

Branch: feature/GL-111-producerPartitionKey

Author: [Vito](https://gitlab.com/vito.ressa)

Closes #111 


## WASP 2.8.0
30/03/2018

### Resolve "Error/exception handling - Pipegraph start may leave components running while returning a failure"

[Merge request 29](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/29)

Created at: 2018-03-12T16:41:58.997Z

Updated at: 2018-03-30T14:03:08.080Z

Branch: feature/13-error-exception-handling-pipegraph-start-may-leave-components-running-while-returning-a-failure

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

#### Start Pipegraph
When a REST start pipegraph request is received a new pipegraphinstance is created and asynchronously runned by the assigned pipegraph guardian

```bash
http -v POST :2891/pipegraphs/TestConsoleWriterWithMetadataStructuredJSONPipegraph/start

POST /pipegraphs/TestConsoleWriterWithMetadataStructuredJSONPipegraph/start HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate
Connection: keep-alive
Content-Length: 0
Host: localhost:2891
User-Agent: HTTPie/0.9.9


HTTP/1.1 200 OK
Content-Length: 97
Content-Type: application/json
Date: Fri, 30 Mar 2018 09:32:05 GMT
Server: akka-http/10.0.9

{
    "Result": "OK", 
    "data": "Pipegraph 'TestConsoleWriterWithMetadataStructuredJSONPipegraph' accepted (queued or processing)"
}
```

#### Check pipegraph instance status

* To check if a pipegraph started check the status of the instance via a REST the request /pipegraph/$name/instances

```
http -v GET :2891/pipegraphs/TestConsoleWriterWithMetadataStructuredJSONPipegraph/instances            531ms  ven 30 mar 2018 15:04:29 CEST
GET /pipegraphs/TestConsoleWriterWithMetadataStructuredJSONPipegraph/instances HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate
Connection: keep-alive
Host: localhost:2891
User-Agent: HTTPie/0.9.9



HTTP/1.1 200 OK
Content-Length: 285
Content-Type: application/json
Date: Fri, 30 Mar 2018 13:04:37 GMT
Server: akka-http/10.0.9

{
    "Result": "OK", 
    "data": [
        {
            "currentStatusTimestamp": 1522404178417, 
            "instanceOf": "TestConsoleWriterWithMetadataStructuredJSONPipegraph", 
            "name": "TestConsoleWriterWithMetadataStructuredJSONPipegraph-c5db85bb-08d0-4d5a-ab7f-2f52019ddc1d", 
            "startTimestamp": 1522404178360, 
            "status": "PROCESSING"
        }
    ]
}

```

#### Stop Pipegraph

* To Stop a pipegraph use the REST api, the stop will be performed asynchronously, check the status via instances api

```
http -v POST :2891/pipegraphs/TestConsoleWriterWithMetadataStructuredJSONPipegraph/stop                274ms  ven 30 mar 2018 15:04:37 CEST
POST /pipegraphs/TestConsoleWriterWithMetadataStructuredJSONPipegraph/stop HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate
Connection: keep-alive
Content-Length: 0
Host: localhost:2891
User-Agent: HTTPie/0.9.9



HTTP/1.1 200 OK
Content-Length: 97
Content-Type: application/json
Date: Fri, 30 Mar 2018 13:05:58 GMT
Server: akka-http/10.0.9

{
    "Result": "OK", 
    "data": "Pipegraph 'TestConsoleWriterWithMetadataStructuredJSONPipegraph' stopped"
}

http -v GET :2891/pipegraphs/TestConsoleWriterWithMetadataStructuredJSONPipegraph/instances            304ms  ven 30 mar 2018 15:05:58 CEST
GET /pipegraphs/TestConsoleWriterWithMetadataStructuredJSONPipegraph/instances HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate
Connection: keep-alive
Host: localhost:2891
User-Agent: HTTPie/0.9.9



HTTP/1.1 200 OK
Content-Length: 282
Content-Type: application/json
Date: Fri, 30 Mar 2018 13:06:53 GMT
Server: akka-http/10.0.9

{
    "Result": "OK", 
    "data": [
        {
            "currentStatusTimestamp": 1522415158518, 
            "instanceOf": "TestConsoleWriterWithMetadataStructuredJSONPipegraph", 
            "name": "TestConsoleWriterWithMetadataStructuredJSONPipegraph-c5db85bb-08d0-4d5a-ab7f-2f52019ddc1d", 
            "startTimestamp": 1522404178360, 
            "status": "STOPPED"
        }
    ]
}

```

#### Phases

When a pipegraph is started a new `PipegraphGuardian` is spawned by the `SparkConsumersStreamingMasterGuardian`
the `PipegraphGuardian` instantiate an `StructuredStreamingETLActor` for each Etl component

The `PipegraphGuardian` manages the `StructuredStreamingETLActors` in steps

##### Activation
The strategy is applied, if a failure happens in strategy application it is assumed as transient and thus Activation is retried until successful or a Stop is received (maybe a rest request from within the code executed in wasp jvm before strategy application) 
##### Materialization
The output plugin is applied, if a failure happens in output plugin it is assumed as transient and thus Materialization is retried until successful or a Stop is received (maybe the output data store did not accept the creation of the tables/indices)
##### Monitoring
The streaming query is monitored for failure and progress, if the query signal that a failure and that it has stopped then the activation of that query is retried (the other etls are left running)
##### Stop
The streaming query are stopped gracefully one by one
 

#### Availability guarantees

If spark-consumers jvm is lost (the spark driver is also lost) spark checkpointing and kafka should handle the buffering of incoming data and the consistency of the streaming. 
When the  spark-consumers jvm restarts it should check mongo for pipegraphs in pending or processing status and restart them recovering the queries that were running when the unexpected shutdown occurred

`NOTE: To prevent reactivation of running pipegraphs when intentionally rebooting spark-consumers explicitly stop all pipegraphs`


![statemachines](documentation/diagrams/statemachines.png)
```plantuml
@startuml
state PipegraphGuardian {
[*] --> WaitingForWork
WaitingForWork --> RequestingWork: WorkAvailable
RequestingWork --> RequestingWork : WorkNotGiven
RequestingWork --> Activating : WorkGiven
Activating --> Activating : ActivateETL
Activating --> Activating : ETLActivated
Activating --> Activating : ETLNotActivated
Activating --> Activated : ActivationFinished
Activated --> Stopping : CancelWork
Activated --> Materializing : MaterializePipegraph
Materializing --> Materializing : MaterializeETL
Materializing --> Materializing : ETLNotMaterialized
Materializing --> Materializing : ETLMaterialized
Materializing --> Materialized : MaterializationFinished
Materialized --> Stopping  : CancelWork
Materialized --> Monitoring : MonitorPipegraph
Monitoring --> Monitoring : CheckETL
Monitoring --> Monitoring : ETLCheckSucceeded
Monitoring --> Monitoring : ETLCheckFailed
Monitoring --> Monitored: MonitoringFinished
Monitored --> Stopping : CancelWork
Monitored --> Monitoring : MonitorPipegraph
Monitored --> Activating
Stopping --> Stopping: StopETL
Stopping --> Stopping: ETLNotStopped
Stopping --> Stopping: ETLStopped
Stopping --> Stopped : StopFinished
Stopped --> [*]: Shutdown
}


state StructuredStreamingETLActor {
[*] --> WaitingForActivation
WaitingForActivation --> WaitingForMaterialization : ActivateETL
WaitingForActivation --> [*]: CancelWork
WaitingForMaterialization --> WaitingForMonitoring : MaterializeETL
WaitingForMaterialization --> [*]: CancelWork
WaitingForMonitoring --> WaitingForMonitoring : CheckETL
WaitingForMonitoring --> [*]: CancelWork
}

state SparkConsumersStreamingMasterGuardian {
[*] --> Idle
Idle --> Initializing: Initialize
Initializing --> Initializing: TimeOut
Initializing --> Initialized

}
@enduml
```

### Resolve "Verify checkpointing with new strategy load"

[Merge request 43](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/43)

Created at: 2018-03-22T11:36:44.193Z

Updated at: 2018-03-29T16:39:14.656Z

Branch: feature/4-verify-checkpointing-with-new-strategy-load

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #4, #118 

*  Default (`reference.conf`) CheckpointDir root is `/checkpoint` on HDFS
*  New testcases to test and show "best-practise" for checkpoint using stateful transformations in Spark StructuredStreaming ETL (e.g. `flatMapGroupsWithState`); in `whitelabel/models/test/TestPipegraphs.scala`
   1.  `TestCheckpointConsoleWriterStructuredJSONPipegraph`
   2.  `TestCheckpointConsoleWriterStructuredAVROPipegraph`
* Documentation
   1. `documentation/spark-structured-streaming-checkpointing.md`: general info about checkpoint internal implementation
   2. `whitelabel/README.md`: added section "Checkpoint and Stateful transformation in Spark StructuredStreaming ETL"

### Resolve "[cherrypick] hotfix kafka-writer-config"

[Merge request 48](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/48)

Created at: 2018-03-30T10:25:42.227Z

Updated at: 2018-03-30T12:29:07.699Z

Branch: feature/119-cherrypick-hotfix-kafka-writer-config

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #119 

*  Fixed some Kafka configurations not being used properly.
*  Fixed reference.conf in wasp core to reflect those changes.


## WASP 2.9.0
06/04/2018

### Resolve "[config] production checks"

[Merge request 49](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/49)

Created at: 2018-04-03T09:45:04.610Z

Updated at: 2018-04-05T10:19:15.583Z

Branch: feature/105-config-production-checks

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #105

Added management of production checks using validation rules:
*  `environment.validationRulesToIgnore`: list of validation rules to ignore (through validation rule's keys)
* For all not ignored validation rules: print VALIDATION-RESULT (validation rule's keys and PASSED/NOT PASSED); if there is at least a validation failure (NOT PASSED):
 * `environment.mode` == "develop": print VALIDATION-WARN and continue
 * `environment.mode` != "develop" (all not "develop" is considered "production" by default): print VALIDATION-ERROR and exit


Documentation
* `whitelabel/README.md`: added section "Configuration validation rules"

### Resolve "Change rest api message for pipegraph stop to better explain that stopping is async"

[Merge request 51](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/51)

Created at: 2018-04-05T10:39:13.190Z

Updated at: 2018-04-05T10:52:10.873Z

Branch: feature/120-change-rest-api-message-for-pipegraph-stop-to-better-explain-that-stopping-is-async

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #120 

Updated feedback messages at start/stop of batch_jobs/pipegraphs (accepted/not accepted due to the new concept of "instance")

### Resolve "WaspKafkaWriter and KafkaWriter issues""

[Merge request 53](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/53)

Created at: 2018-04-05T15:21:51.837Z

Updated at: 2018-04-06T14:21:48.220Z

Branch: feature/74-waspkafkawriter-has-a-hardcoded-value-for-request-required-acks

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #74, #123 

Kafka config "acks":
* taken out from "kafka.others.acks" to "kafka.acks"
* default (`reference.conf`): -1 (i.e. "all")

Kafka config "others":
* Must contain duplicated keys (for WASP-Producers and for PipegraphETLKafka-Producers/Consumers). Example:

    ```
        others = [
          { "security.protocol" = "SASL_PLAINTEXT" }
          { "sasl.kerberos.service.name" = "kafka" }
          { "sasl.jaas.config" = "com.sun.security.auth.module.Krb5LoginModule required storeKey=true useKeyTab=true useTicketCache=false keyTab=\"./wasp2.keytab\" serviceName=\"kafka\" principal=\"wasp2@REALM\";" }
          { "sasl.mechanism" = "GSSAPI" }
          { "kafka.security.protocol" = "SASL_PLAINTEXT" }
          { "kafka.sasl.kerberos.service.name" = "kafka" }
          { "kafka.sasl.jaas.config" = "com.sun.security.auth.module.Krb5LoginModule required storeKey=true useKeyTab=true useTicketCache=false keyTab=\"./wasp2.keytab\" serviceName=\"kafka\" principal=\"wasp2@REALM\";" }
          { "kafka.sasl.mechanism" = "GSSAPI" }
        ]
    ```


## WASP 2.10.0
13/04/2018

### Resolve "Telemetria tramite StructuredStreaming"

[Merge request 50](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/50)

Created at: 2018-04-04T14:09:17.647Z

Updated at: 2018-04-13T13:46:11.977Z

Branch: feature/1-telemetria-tramite-structuredstreaming

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Closes #1 

#### Flow


```
+---------------------------------------------------------------------------------------+
|                                                                                       |
|                                                  wasp-consumer-streaming              |
|                                                                                       |
|  +-----------------------------------+                                                |
|  |                                   |                                                |
|  | StructuredStreamingMasterGuardian |                                                |
|  |                                   |                                                |
|  +-----------------------------------+                                                |
|                                                                                       |
|  +-------------------------------------------------------------------------------+    |
|  |      +-------------------+                                                    |    |
|  |      |                   |                              Pipegraph             |    |
|  |      | PipegraphGuardian |                                                    |    |
|  |      |                   |                                                    |    |          TELEMETRY
|  |      +-------------------+                                                    |    |
|  |                                                                               |    |
|  | +------------------------------------------------------------+                |    |
|  | |                                                            |                |    |
|  | |   +-----------------------------+            ETLBlock      |                |    |
|  | |   |                             |                          |                |    |
|  | |   | StructuredStreamingETLActor |                          |                |    |
|  | |   |                             |                          |                |    |
|  | |   +-----------------------------+                          |                |    |
|  | |                                                            |                |    |
|  | |                                                            |                |    |
|  | |                                                            |                |    |
|  | |         +----------------+                                 |                |    |
|  | |         |                |                                 |                |    |
|  | |         | TelemetryActor |                                 |                |    |
|  | |         |                |                                 |                |    |
|  | |         +------|X|-------+                                 |                |    |
|  | +----------------|X|-----------------------------------------+                |    |
|  +------------------|X|----------------------------------------------------------+    |
+---------------------|X|---------------------------------------------------------------+
                       |
                       |
                       |
                       |
              +--------v--------+    +--------------------+       +-----------------+
              |                 |    |                    |       |                 |
              | telemetry.topic +----> TelemetryPipegraph +-------> OutputDataStore |
              |                 |    |                    |       |                 |
              +--------^--------+    +--------------------+       +-----------------+
                       |
                       |
                       |
                       |
     +-----------------X--------------------------------------------------------+
     |                 +                                                        |
     |                 |                                        SPARK-WORKER    |
     |                 |                                                        |
     |                 |                                                        |
     |                 |                                                        |
     |   +-------------X-----------------------------------------+              |
     |   |             |                                         |              |
     |   |             |                      PARTITION-N        |              |
     |   |   +---------+------+                                  |              |
     |   |   |                |                                  |              |
     |   |   |  Kafka Writer  |                                  |              |
     |   |   |                |                                  |              |           LATENCY
     |   |   +----------------+                                  |              |
     |   |                                                       |              |
     |   |                                                       |              |
     |   +-------------------------------------------------------+              |
     |                                                                          |
     +--------------------------------------------------------------------------+

```

Telemetry is written to `elasticsearch` if `wasp.datastore.indexed="elastic"` or to `solr` if `wasp.datastore.indexed="solr"`


#### Streaming query Telemetry

**To enable telemetry collection Start TelemetryPipegraph or enable system pipegraph startup**

Telemetry about streaming query performance is now collected by `TelemetryActor`, a child of `StructuredStreamingETLActor`


Telemetry is extracted during the Monitoring phase of a pipegraph, extracted telemetry is composed of:

* inputRows
* inputRowsPerSecond
* processedRowsPerSecond
* durationMS of various spark streaming events

```json
{
    "messageId": "22cc1baf-b28e-4385-995d-c7db4e040f5c",
    "timestamp": "2018-04-09T16:54:10.652Z",
    "sourceId": "pipegraph_Telemetry Pipegraph_structuredstreaming_write_on_index_writer_telemetry_elastic_index",
    "metric": "triggerExecution-durationMs",
    "value": 1
  }
```

sourceId is the name of the streaming query, messageId is relative to the collection of the telemetry and is a random identifier.

#### Message Latency sampling

Latency data is extracted per message with a subsampling applied per partition.

The default sampling is one every 100 messages for partition.

**To adjust the sampling factor provide the strategy with a `Configuration` object containing the key**
```wasp.telemetry.latency.sample-one-message-every=<number of messages>```

To enable latency collection the DataFrame should have a `Metadata` column and the ETL block should have a declared strategy.

latency is extracted in the form

```json
{
    "messageId": "3729",
    "timestamp": "2018-04-09T16:49:17.349Z",
    "sourceId": "testDocumentWithMetadataProducer/test-with-metadata-console-etl-enter/test-with-metadata-console-etl-exit",
    "metric": "latencyMs",
    "value": 1
  }
```

messageId is assigned by the source by generating a `metadata` column, see 
`it.agilelab.bigdata.wasp.whitelabel.producers.test.TestDocumentWithMetadataProducerGuardian`

sourceId is the path covered by the message inside wasp

```

testDocumentWithMetadataProducer/test-with-metadata-console-etl-enter
                   ^                              ^
                  source                        enter in streaming query

the timestamp will be the one recorded at the entrance of an etl block, latencyMS will be the difference between the time of
publish on kafka and the entrance of the message in an etl block (we will measure latency to dequeue from kafka)

testDocumentWithMetadataProducer/test-with-metadata-console-etl-enter/test-with-metadata-console-etl-exit
                   ^                               ^                                       ^
                source                        enter in streaming query               exit from streaming query

the timestamp will be the one recorded at the exit of an etl block, latencyMS will be the difference between the time of
enter and the time of exit from the etl block (we will measure latency in processing)

```

```scala
var counter = 0
        
        partition.map { row =>

          if(counter % samplingFactor == 0) {

            val metadata = row.getStruct(row.fieldIndex("metadata"))

            val pathField = metadata.fieldIndex("path")

            val messageId = metadata.getString(metadata.fieldIndex("id"))

            val sourceId = metadata.getString(metadata.fieldIndex("sourceId"))

            val arrivalTimestamp = metadata.getLong(metadata.fieldIndex("arrivalTimestamp"))

            val path = Path(sourceId,arrivalTimestamp) +: metadata.getSeq[Row](pathField).map(Path.apply)

            val lastTwoHops = path.takeRight(2)

            val latency = lastTwoHops(1).ts - lastTwoHops(0).ts

            val collectionTimeAsString = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(lastTwoHops(1).ts))

            val compositeSourceId = path.map(_.name.replace(' ', '-')).mkString("/")


            val json = JSONObject(Map("messageId" -> messageId,
                                      "sourceId" -> compositeSourceId,
                                      "metric" -> "latencyMs",
                                      "value" -> latency,
                                      "timestamp" -> collectionTimeAsString)).toString(JSONFormat.defaultFormatter)


            val topic = SystemPipegraphs.telemetryTopic.name

            val record = new ProducerRecord[Array[Byte], Array[Byte]](topic,
                                                                      messageId.getBytes(StandardCharsets.UTF_8),
                                                                      json.getBytes(StandardCharsets.UTF_8))
            writer.send(record)

          }

          counter = counter + 1
          row
        }
```



### Resolve "[docker] updates in order to use Spark Standalone"

[Merge request 54](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/54)

Created at: 2018-04-06T09:29:09.623Z

Updated at: 2018-04-09T16:41:47.430Z

Branch: feature/122-docker-update-spark-images-to-2-2-1-to-test-spark-standalone

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #122 

**In order to enable usage on Spark Standalone cluster manager:**

1.  Updated Spark docker-images (`spark-docker-compose.yml`):
 *  image: from `gettyimages/spark:2.2.0-hadoop-2.7` to `gettyimages/spark:2.2.1-hadoop-2.7`
 *  Worker SPARK_WORKER_MEMORY: from `2048m` to `4096m`

2.  Added to `reference.conf` / `whitelabel/docker/docker-environment.conf`: `cores.max`

3. Added Validation Rules to check that config `cores.max` >= `executor-cores` when `master.protocol=="spark`" within `spark-streaming`/`spark-batch`

4. Whitelabel uses Spark Standalone cluster manager by default:
 * `whitelabel/docker/docker-environment.conf` -> `master` within `spark-streaming`/`spark-batch`

### Resolve "Instance info on start"

[Merge request 57](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/57)

Created at: 2018-04-09T15:46:36.119Z

Updated at: 2018-04-13T10:26:47.369Z

Branch: feature/instance-info-on-start

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #124, #126 

* Returned `instanceId` of the created instance on start of batchjobs
```javascript
{
    "Result": "OK",
    "data": {
        "startResult": "Batch job 'TestBatchJobFromHdfsFlatToConsole' start accepted'",
        "instance": "TestBatchJobFromHdfsFlatToConsole-96ade2b0-7d20-4c21-b7ca-797a93c1a355"
    }
}
```

* Returned `instanceId` of the created instance on start of pipegraphs
```javascript
{
    "Result": "OK",
    "data": {
        "startResult": "Pipegraph 'TestConsoleWriterStructuredJSONPipegraph' start accepted'",
        "instance": "TestConsoleWriterStructuredJSONPipegraph-1ebc889c-8c71-449b-943d-ca9fd5181598"
    }
}
```

* Updated tests: `SparkConsumersBatchMasterGuardianSpec.scala` and `IntegrationSpec.scala`
* Updated `REST.md`


### Resolve "REST API to retrieve instance status"

[Merge request 58](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/58)

Created at: 2018-04-10T08:36:49.022Z

Updated at: 2018-04-13T10:01:39.111Z

Branch: feature/rest-api-to-retrieve-instance-status

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #125, #127 

- Implemented REST to retrieve batchjob instance status

```bash
curl -X GET \
  http://localhost:2891/batchjobs/TestBatchJobFromHdfsFlatToConsole/instances/TestBatchJobFromHdfsFlatToConsole-8a900d14-3859-4a5a-b2c2-5b8fcb8250c4
```
```javascript
{
    "Result": "OK",
    "data": {
        "name": "TestBatchJobFromHdfsFlatToConsole-8a900d14-3859-4a5a-b2c2-5b8fcb8250c4",
        "instanceOf": "TestBatchJobFromHdfsFlatToConsole",
        "restConfig": {
            "intKey2": 5,
            "stringKey": "aaa"
        },
        "currentStatusTimestamp": 1523437338661,
        "error": "java.lang.Exception: Failed to create data frames for job TestBatchJobFromHdfsFlatToConsole...",
        "status": "FAILED",
        "startTimestamp": 1523437333307
    }
}
```

- Implemented REST to retrieve pipegraph instance status

```bash
curl -X GET \
  http://localhost:2891/pipegraphs/TestConsoleWriterStructuredJSONPipegraph/instances/TestConsoleWriterStructuredJSONPipegraph-6e139f53-254c-44b9-8b6a-3dbbaaa84760
```
```javascript
{
    "Result": "OK",
    "data": {
            "name": "TestConsoleWriterStructuredJSONPipegraph-6e139f53-254c-44b9-8b6a-3dbbaaa84760",
            "instanceOf": "TestConsoleWriterStructuredJSONPipegraph",
            "currentStatusTimestamp": 1523435670321,
            "status": "PROCESSING",
            "startTimestamp": 1523435670306
    }
}
```

- Updated `REST.md`

### Resolve "[kafka writer] possibility to define kafka partition key from nested dataframe field"

[Merge request 59](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/59)

Created at: 2018-04-11T17:24:37.695Z

Updated at: 2018-04-13T13:47:18.852Z

Branch: feature/112-kafka-writer-possibility-to-define-kafka-partition-key-from-nested-dataframe-field

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #112

Supported kafka partition key from nested dataframe field for AVRO and JSON

See `whitelabel`:
*  `TestPipegraphs.JSON.Structured.kafka`, `TestTopicModel.json2`
*  `TestPipegraphs.AVRO.Structured.kafka`, `TestTopicModel.avro2`


## WASP 2.11.0
20/04/2018

### Resolve "[Documentation] Draft Procedure Operations"

[Merge request 60](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/60)

Created at: 2018-04-16T13:44:38.295Z

Updated at: 2018-04-20T09:24:21.904Z

Branch: feature/22-documentation-procedure-operations

Author: [Davide Colombatto](https://gitlab.com/davidecolombatto)

Closes #22

All the sub-documentation, diagrams and icons are moved under `documentation`

Revised documentation:
* WASP RESTful APIs -> `api.md`
* Development -> `dev.md`, includes old `whitelabel.md`, `yarn.md` and `kerberos.md`

Draft documentation:
* Operations -> `ops.md`

Revised code:
*  API `http://localhost:2891/help` to retrieve the list of available APIs
*  `SolrAdminActor` checks `System.getProperty("java.security.auth.login.config")` instead of 
   `System.getenv("java.security.auth.login.config")`

## WASP 2.12.0
10/05/2018

### Batch jobs can now be multi-instantiated

[Merge request 61](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/61)

Created at: 2018-05-10T15:58:51.154Z

Updated at: 2018-05-10T16:13:44.280Z

Branch: feature/GL-130-parallelBatchJobs

Author: [Vito](https://gitlab.com/vito.ressa)

GL-130: Batch jobs can now be multi-instantiated. Exclusivity can be limited to a subset of rest parameters.
BatchJobModel now has a "exclusivityConfig" of type BatchJobExclusionConfig.
BatchJobExclusionConfig has 2 fields:
- isFullyExclusive: Boolean
- restConfigExclusiveParams: Seq[String]
If a batch job is fully exclusive it can never be multi-instantiated as pending or processing. Eventual start request fails if there is another job of the same model pending or processing.

If a batch job is NOT fully exclusive but restConfigExclusiveParams is NOT empty can be multi-instantiated if the instances differs in input rest config in at least one fields indicated in restConfigExclusiveParams.

If a batch job is NOT fully exclusive and restConfigExclusiveParams is empty it can always be multi-instantiated.

## WASP 2.12.3
20/06/2018

[Merge request 62](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/62)

Branch: hotfix/mongoAuthentication

Author: [Vito](https://gitlab.com/vito.ressa)

HOTFIX: Mongo authentication fixed when mongo.username is defined.

Connection to authorized mongo fixed.
New fields in config:
wasp.mongo.username -> Username to be used to connect
wasp.mongo.password -> Password to be used to connect

If wasp.mongo.username = "" authentication doesn't happen


## WASP 2.12.4
02/07/2018

[Merge request 63](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/63)

Branch: hotfix/hbaseOptConfigs

Author: [Vito](https://gitlab.com/vito.ressa)

Now it is possible to add options to hbase config


## WASP 2.13.0

[Merge request 64](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/64)

Branch: feature/GL-131-restHttps

Author: [Vito](https://gitlab.com/vito.ressa)

Is now possible to expose REST API via https. 
In config put ssl config under wasp.rest.server.https 

```
[ ... ] 
https = { 
          active = true 
          keystore-location = "/path/to/test-file.p12" 
          password-location = "/path/to/keystore-pwd.key" 
          keystore-type = "PKCS12" 
} 
[ ... ]
```

## WASP 2.14.0

### kafka plaintext support

[Merge request 65](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/65)

Created at: 2018-08-07T12:39:34.186Z

Updated at: 2018-08-07T12:52:29.932Z

Branch: feature/kafka-plaintext-support

Author: [Stefano Castoldi](https://gitlab.com/theShadow89)

Add `plaintext` for Kafka Topic Data Type
*  In the `Producer Actor` implementations, the method `generateOutputJsonMessage` should return a simple string instead of a JSON when topic data type is `plaintext`

# WASP 2.15.0

### Resolve "Spark consumers streaming module is unable to proceed if spark context goes down due to executor failures"

[Merge request 66](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/66)

Created at: 2018-08-08T10:15:54.270Z

Updated at: 2018-08-14T09:19:11.063Z

Branch: hotfix/132-spark-consumers-streaming-module-is-unable-to-proceed-if-spark-context-goes-down-due-to-executor-failures

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Closes #132

### allow to discard column metadata before writer write data

[Merge request 67](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/67)

Created at: 2018-08-13T15:44:16.767Z

Updated at: 2018-08-14T09:19:47.367Z

Branch: hotfix/dropColumnMetadata

Author: [Andrea L.](https://gitlab.com/andreaL)

Allow to discard column metadata before writing phase begins. For use this feature you need to set the property 'dropMetadata' in strategy config. 

Added the possibility to select a datastore for the telemetry index.

# WASP 2.15.2

### Resolve "Actor names are not unique when restarting (actor restart are async, append unique identifier)"

[Merge request 69](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/69)

Created at: 2018-08-22T12:44:42.151Z

Updated at: 2018-08-22T13:10:03.147Z

Branch: hotfix/133-actor-names-are-not-unique-when-restarting-actor-restart-are-async-append-unique-identifier

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Closes #133

An unique uuid will be appended to every actor name in order to prevent name clashes if akka has not yet deregistered the old actor name

# WASP 2.16.0

### Change Spark version to CDS

[Merge request 70](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/70)

Created at: 2018-08-28T15:32:59.897Z

Updated at: 2018-09-18T10:22:45.866Z

Branch: feature/136-spark-cdh

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #136 

WASP now uses the CLoudera Distribution of Spark, which means that we get CDH as a transitive dependency. This should fix various problems with KMS and also bring many other fixes and improvementsfrom CDS & CDH.

### Resolve "Add ability to specify query/writer options for each ETL/Writer"

[Merge request 71](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/71)

Created at: 2018-08-28T16:00:11.317Z

Updated at: 2018-09-18T14:37:54.988Z

Branch: feature/138-query-writer-options-for-each-etl-writer

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #138

New features and improvements:
- the streaming readers of a SparkStructuredStreamingETLModel have been moved to a separate field in preparation of the support of stream-to-stream joins and multi-stream support
- StreamingReaderModel has been introduced to add support for rate limiting each streaming source independently
- trigger interval is now independently configurable for each SparkStructuredStreamingETLModel
- datastores are now organized using a class hierarchy (see DatastoreCategory, DatastoreProduct and related)
- WriterModels now don't have an optional endpoint anymore
- docker containers for services are now based on CDH 5.12
- docker containers for WASP now use the JDK 1.8u181
- query final configuration and starting has been moved from the writers to MaterializationSteps 
- general rework of the Spark consumers plugins and writers to accomodate the new features
- the telemetry writer can now be specified as "default", letting the framework choose the indexed datastore from its configuration

Fixes:
- remove kafkaAccessType in SparkStructuredStreamingETLModel as it is not applicable
- some Spark Structured Streaming writers did not apply the trigger interval setting
- improvements in some logging messages
- the whitelabel now has a functional configuration for the telemetry
- fix kafka docker container broker znode check


### Resolve "Add informations on how to contribute"

[Merge request 74](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/74)

Created at: 2018-08-29T16:53:22.034Z

Updated at: 2018-09-18T10:23:06.997Z

Branch: feature/137-contributing

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #137

### Resolve "Solr Writer Atomic Update Support"

[Merge request 75](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/75)

Created at: 2018-09-03T09:03:41.415Z

Updated at: 2018-09-18T10:34:51.176Z

Branch: feature/144-solr-writer-atomic-update-support

Author: [Gloria Lovera](https://gitlab.com/glovera)

Assignee: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #144

New features and improvements:
- the Solr writers now support atomic updates

When processing Rows, any fields of type MapType are converted to a Java Map that is then interpreted by Solr as an atomic update with the modifier specified as the key. To perform an atomic add (as in, append to a multivalue) on a multivalue field named `ts` with values of type `Timestamp`, you would declare a column of a DataFrame called `ts` with contents:
```
Map[String,Timestamp]("add"-> new Timestamp(System.currentTimeMillis)))
```

### Resolve "Change Spark version to CDS 2.2.0.cloudera2"

[Merge request 76](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/76)

Created at: 2018-09-03T10:31:53.030Z

Updated at: 2018-09-18T10:24:37.179Z

Branch: feature/145-change-spark-version-to-cds-2-2-0-cloudera2

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #145

### Resolve "Fix setup.py for wasprng"

[Merge request 77](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/77)

Created at: 2018-09-18T15:27:03.893Z

Updated at: 2018-09-18T15:44:16.360Z

Branch: feature/147-fix-setup-py-for-wasprng

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #147

# WASP 2.17.0

### Resolve "Add ability to specify a different destination topic per each row in the strategies"

[Merge request 78](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/78)

Created at: 2018-09-18T17:06:40.014Z

Updated at: 2018-10-01T10:20:07.001Z

Branch: feature/142-add-ability-to-specify-a-different-destination-topic-per-each-row-in-the-strategies

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #142

#### New features and improvements

- the Spark Structured Streaming Kafka writer now supports wrtiting to a different topic on a per-row basis using a MultiTopicModel instead of a TopicModel as the DatastoreModel for the WriterModel

### Resolve "Support raw bytes as Kafka output format"

[Merge request 81](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/81)

Created at: 2018-09-18T17:16:59.400Z

Updated at: 2018-10-01T17:25:04.565Z

Branch: feature/149-support-raw-bytes-as-kafka-output-format

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #149

Closes #157

#### New features and improvements

- WASP now supports a new topic data type, "binary", for directly reading and writing binary data to/from Kafka when using the Producers and Spark Structured Streaming

#### Bug fixes

- fixed reading and writing with "plaintext" topic data type support in Producers and Spark Structured Streaming


### Resolve "Support subscribing to multiple topics form a single streaming input"

[Merge request 82](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/82)

Created at: 2018-09-20T08:55:12.108Z

Updated at: 2018-09-30T18:28:16.180Z

Branch: feature/150-suport-subscribing-to-multiple-topics-form-a-single-streaming-input

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #150

#### New features and improvements

- the Spark Structured Streaming Kafka reader now supports reading from multiple topics at once using a MultiTopicModel instead of a TopicModel as the DatastoreModel for the StreamingReaderModel

### Resolve "Support for Kafka message headers"

[Merge request 83](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/83)

Created at: 2018-09-20T09:17:50.323Z

Updated at: 2018-10-01T17:34:17.559Z

Branch: feature/151-support-for-kafka-message-headers

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #151

#### Breaking changes:

- the Kafka Spark reader/writer code has been moved to the plugin-kafka-spark module. TO use Kafka you will have to add the wasp-plugin-kafka-spark artifact to the dependencies of your consumer-spark module
- the Kafka version has changed from 0.10.2.1 to 0.11.0-kafka-3.0.0. WASP is now based on the Cloudera Distribution of Kafka.
- in order to supports Kafka message headers (added in 0.11 with KIP-82), the Kafka Spark Structured Streaming reader now also returns all message metadata into a "kafkaMetadata" column: you may have to explicitly drop it, which for no-strategy kafka-to-kafka ETLs means adding a strategy
- the type of the value column returned by the plaintext format changed from bytes to actual text: you may have to remove any explicit conversion/casting that you did in the strategies


#### New features and improvements:

- the Kafka Spark Structured Streaming writer now supports Kafka message headers (added in 0.11 with KIP-82)
- the Kafka Spark Structured Streaming reader now supports Kafka message headers (added in 0.11 with KIP-82)

### Resolve "Update Docker image for Kafka service to >= 0.11"

[Merge request 86](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/86)

Created at: 2018-09-30T08:01:44.270Z

Updated at: 2018-09-30T08:11:13.874Z

Branch: feature/155-update-docker-image-for-kafka-service-to-0-11

Author: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #155

#### New features and improvements

- the Kafka service container now runs Kafka 0.11.0.3

### Resolve "Get informations about Streaming Query in Spark Structured Streaming"

[Merge request 90](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/90)

Created at: 2018-10-15T08:20:50.323Z

Updated at: 2018-10-15T10:28:17.559Z

Branch: feature/166-add-informations-about-sources-in-streaming

Author: [Eugenio Liso](https://gitlab.com/Warrior92)

Now the Telemetry actor can send the TelemetryMessageSource message (that contains informations about the Streaming Query) to another actor, in order to allow the analysis of the Streaming Query. The Telemetry actor sends a message on a publishSubscribe topic regularly and, when it receives a connection from another actor, it will begin to send the TelemetryMessageSource message also to the other actor.


# WASP 2.19.0

### Feature/168 schema registry

[Merge request 99](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/99)

Created at: 2018-11-06T21:41:14.743Z

Updated at: 2018-11-08T18:31:12.615Z

Branch: feature/168-schema-registry

Author: [Andrea L.](https://gitlab.com/andreaL)

Integration of Darwin in WASP.

To use darwin in our project  it is necessary to add a connector implementation.
In this release two connectors are supported, you have to choose one of them:

 1) `"it.agilelab" %% "darwin-hbase-connector" % "1.0.3"`
 2) `"it.agilelab" %% "darwin-postgres-connector" % "1.0.3"`


The chosen connector should be added to the following modules of your project xxx:
- `xxx-master`
- `xxx-producers`
- `xxx-consumers-spark`

Here an example:

```scala
lazy val whiteLabelMaster = Project("wasp-whitelabel-master", file("whitelabel/master"))
	.settings(Settings.commonSettings: _*)
	.dependsOn(whiteLabelModels)
	.dependsOn(master)
	.dependsOn(plugin_hbase_spark)
	.settings(libraryDependencies ++= Dependencies.log4j :+ Dependencies.darwinHBaseConnector)
	.enablePlugins(JavaAppPackaging)
```

If you use `darwin-hbase-connector`, it is necessary to add `dependsOn(plugin_hbase_spark)` in each module.

In your configuration file add the following option under the path `wasp` (insert only configuration for connector chosen)
```
  #possible value is hbase or postgres
  darwinConnector = "" 
  avroSchemaManager {
    wasp-manages-darwin-connectors-conf = #true/false

    #darwin {                                       #hbase-conf
      #namespace  = "AVRO"                          #optional
      #table      = "SCHEMA_REPOSITORY"             #optional
      #hbaseSite  = "path/to/hbase-site-xml"        #required if wasp-manages-darwin-connectors-conf= false
      #coreSite   = "path/to/core-site-xml" 	    #required if wasp-manages-darwin-connectors-conf= false
      #isSecure   = true/false                      #required if wasp-manages-darwin-connectors-conf= false
      #principal  = "user@REALM"                    #required if wasp-manages-darwin-connectors-conf= false
      #keytabPath = "path/to/keytab"                #required if wasp-manages-darwin-connectors-conf= false
    #}

    #darwin {                                       #postgres-conf
      #table =                                      #optional
      #host =                                       #required
      #db =                                         #required
      #user =                                       #required
      #password =                                   #required
    #}
  }
```

IF `wasp-manages-darwin-connectors-conf` is set to `true`, it allows WASP to set some configurations in the automatic mode (only for hbase-connector), retrieving from other configurations or from enviornment variables.
E.g for hbase-connector the values `hbaseSite` and `coreSite` will be filled from path `wasp.hbase`, while the `security` option will be filled from these environment variables: `WASP_SECURITY`, `PRINCIPAL_NAME`, `KEYTAB_FILE_NAME`.
Instead if you set `false` this settings will be mandatory in configuration.

The schema can be added to the registry in the `it.agilelab.bigdata.wasp.xxx.master.launcherMasterNodeLauncher` class in the following way:

```scala
  import org.apache.avro.Schema
  object MasterNodeLauncher extends MasterNodeLauncherTrait {

    [...]


    override def launch(commandLine: CommandLine): Unit = {
      super.launch(commandLine)
      addExamplePipegraphs()
      addExampleRegisterAvroSchema()
    }

    private def addExampleRegisterAvroSchema(): Unit = {
        val schemas: Seq[Schema] = Seq(AvroSchema[TopicAvro_v1], AvroSchema[TopicAvro_v2])
        val configAvroSchemaManager = ConfigFactory.parseMap(ConfigManager.getAvroSchemaManagerConfig)
        AvroSchemaManager(configAvroSchemaManager).registerAll(schemas)
    }

    [...]
  }
```

TopicModel and KeyValueModel classes have a new `useAvroSchemaManager` `boolean` field. If this field is set to `true`, the avro will be serialized and deserialized using darwin.

# WASP 2.19.4

### Develop custom Credentials provider for wasp

[Merge request 120](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/120)

Created at: 2019-02-22T16:56:47.465Z

Updated at: 2019-03-01T18:51:01.019Z

Branch: feature/198-authentication-develop-custom-credentials-provider-for-wasp

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

# Delegation Tokens Renewal in spark 2.2


```
                    ┌───────────────────────────┐                ┌────────────────┐                                                                                                                        
     ┌───┐          │Driver                     │                │YARN            │                                                                                                                        
     │KDC│          │(consumers-spark-streaming)│                │Resource manager│                                                                                                                        
     └─┬─┘          └─────────────┬─────────────┘                └───────┬────────┘                                                                                                                        
       │      keytab login        │                                      │                                                                                                                                 
       │<─────────────────────────│                                      │                                                                                                                                 
       │                          │                                      │                                                                                                                                 
       │     kerberos ticket      │                                      │                                                                                                                                 
       │─────────────────────────>│                                      │                                                                                                                                 
       │                          │                                      │                                                                                                                                 
       │                          │        Authenticate with yarn        │                                                                                                                                 
       │                          │──────────────────────────────────────>                                                                                                                                 
       │                          │                                      │                                                                                                                                 
       │                          │           DelegationToken            │                                                                                                                                 
       │                          │<──────────────────────────────────────                                                                                                                                 
       │                          │                                      │                                                                                                                                 
       │                          │Negotiate Application master container│                                                                                                                                 
       │                          │──────────────────────────────────────>                                             │                                                                                   
       │                          │                                      │                                             │                                                                                   
       │                          │                                      │ create and pass delegation token ┌──────────────────────┐                                                                       
       │                          │                                      │ ────────────────────────────────>│SparkApplicationMaster│                                                                       
       │                          │                                      │                                  └──────────┬───────────┘                                                                       
       │                          │          distribute keytab           │                                             │                                                                                   
       │                          │──────────────────────────────────────>                                             │                                                                                   
       │                          │                                      │                                             │                                                                                   
       │                          │                                      │              distribute keytab              │                                                                                   
       │                          │                                      │ ────────────────────────────────────────────>                                                                                   
       │                          │                                      │                                             │                                                                                   
       │                          │                                      │                                             │────┐                                                                              
       │                          │                                      │                                             │    │ Login from keytab                                                            
       │                          │                                      │                                             │<───┘                                                                              
       │                          │                                      │                                             │                                                                                   
       │                          │                                      │                                             │────┐                                                                              
       │                          │                                      │                                             │    │ Renew all delegation token given by yarn                                     
       │                          │                                      │                                             │<───┘                                                                              
       │                          │                                      │                                             │                                                                                   
       │                          │                                      │        negotiate executors container        │                                                                                   
       │                          │                                      │ <────────────────────────────────────────────                                             │                                     
       │                          │                                      │                                             │                                             │                                     
       │                          │                                      │                                        create                                         ┌────────┐                                
       │                          │                                      │ ─────────────────────────────────────────────────────────────────────────────────────>│Executor│                                
       │                          │                                      │                                             │                                         └───┬────┘                                
       │                          │                                    connect back                                    │                                             │                                     
       │                          │<────────────────────────────────────────────────────────────────────────────────────                                             │                                     
       │                          │                                      │                                             │                                             │                                     
       │                          │                                      │                    connect back             │                                             │                                     
       │                          │<──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────                                     
       │                          │                                      │                                             │                                             │                                     
       │                          ────┐                                  │                                             │                                             │                                     
       │                              │ Start credential updater thread  │                                             │                                             │                                     
       │                          <───┘                                  │                                             │                                             │                                     
       │                          │                                      │                                             │                                             │                                     
       │                          │                                      │                                             │                                             │────┐                                
       │                          │                                      │                                             │                                             │    │ Start credential updater thread
       │                          │                                      │                                             │                                             │<───┘                                
       │                          │                                      │                                             │                                             │                                     
       │                          │                                      │                                             │────┐                                        │                                     
       │                          │                                      │                                             │    │ Start credential renewer thread        │                                     
       │                          │                                      │                                             │<───┘                                        │                                     
     ┌─┴─┐          ┌─────────────┴─────────────┐                ┌───────┴────────┐                         ┌──────────┴───────────┐                             ┌───┴────┐                                
     │KDC│          │Driver                     │                │YARN            │                         │SparkApplicationMaster│                             │Executor│                                
     └───┘          │(consumers-spark-streaming)│                │Resource manager│                         └──────────────────────┘                             └────────┘  
```


```
@startuml

participant "KDC" as KDC
participant "Driver\n(consumers-spark-streaming)" as Driver
participant "YARN\nResource manager" as YarnRM
participant "SparkApplicationMaster" as AM
participant "Executor" as Executor

Driver -> KDC: keytab login
KDC -> Driver: kerberos ticket

Driver -> YarnRM: Authenticate with yarn
Driver <- YarnRM : DelegationToken
Driver -> YarnRM: Negotiate Application master container

YarnRM -> AM **: create and pass delegation token

Driver -> YarnRM: distribute keytab
YarnRM -> AM: distribute keytab

AM -> AM: Login from keytab
AM -> AM: Renew all delegation token given by yarn

AM -> YarnRM: negotiate executors container

YarnRM -> Executor **: create

AM -> Driver : connect back
Executor -> Driver: connect back


Driver -> Driver: Start credential updater thread
Executor -> Executor: Start credential updater thread
AM -> AM: Start credential renewer thread


@enduml
```


```
                    ,.-^^-._        ,.-^^-._                                                                                                                                                                            
                   |-.____.-|      |-.____.-|                                                                                                                                                                           
                   |        |      |        |                                                                                                                                                                           
                   |        |      |        |                                                                               ┌────────────────────┐          ┌─────────────────┐          ┌─────────────────┐            
                   |        |      |        |        ┌────────────────────────┐          ┌───────────────────────┐          │[ApplicationMaster] │          │[Driver]         │          │[Executor]       │            
                   '-.____.-'      '-.____.-'        │HBaseCredentialsProvider│          │HdfsCredentialsProvider│          │AMCredentialsRenewer│          │CredentialUpdater│          │CredentialUpdater│            
                     HDFS            HBASE           └───────────┬────────────┘          └───────────┬───────────┘          └─────────┬──────────┘          └────────┬────────┘          └────────┬────────┘            
                      │                │                         │                                   │                                │                              │                            │                     
          ╔═══════╤═══╪════════════════╪═════════════════════════╪═══════════════════════════════════╪════════════════════════════════╪══════════════════════════════╪════════════════════════════╪════════════════════╗
          ║ LOOP  │   │                │                         │                                   │                                │                              │                            │                    ║
          ╟───────┘   │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                                   │                                │────┐                         │                            │                    ║
          ║           │                │                         │                                   │                                │    │ login from keytab       │                            │                    ║
          ║           │                │                         │                                   │                                │<───┘                         │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                            request token                           │                              │                            │                    ║
          ║           │                │                         │ <───────────────────────────────────────────────────────────────────                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │     request token       │                                   │                                │                              │                            │                    ║
          ║           │                │<────────────────────────│                                   │                                │                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │         token           │                                   │                                │                              │                            │                    ║
          ║           │                │────────────────────────>│                                   │                                │                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                                token                               │                              │                            │                    ║
          ║           │                │                         │ ───────────────────────────────────────────────────────────────────>                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                                   │         request token          │                              │                            │                    ║
          ║           │                │                         │                                   │<───────────────────────────────│                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │               request token                                 │                                │                              │                            │                    ║
          ║           │ <────────────────────────────────────────────────────────────────────────────│                                │                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                   token │                                   │                                │                              │                            │                    ║
          ║           │ ────────────────────────────────────────────────────────────────────────────>│                                │                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                                token                               │                              │                            │                    ║
          ║           │                │                         │ ───────────────────────────────────────────────────────────────────>                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │ write token storage file          │                                │                              │                            │                    ║
          ║           │ <──────────────────────────────────────────────────────────────────────────────────────────────────────────────                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                read token storage file                             │                              │                            │                    ║
          ║           │ <────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────│                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │               token storage file content                           │                              │                            │                    ║
          ║           │ ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────>│                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              ────┐                        │                    ║
          ║           │                │                         │                                   │                                │                                  │ load tokens            │                    ║
          ║           │                │                         │                                   │                                │                              <───┘                        │                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                               read token storage file              │                              │                            │                    ║
          ║           │ <─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────│                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                             token storage file content             │                              │                            │                    ║
          ║           │ ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────>│                    ║
          ║           │                │                         │                                   │                                │                              │                            │                    ║
          ║           │                │                         │                                   │                                │                              │                            ────┐                ║
          ║           │                │                         │                                   │                                │                              │                                │ load tokens    ║
          ║           │                │                         │                                   │                                │                              │                            <───┘                ║
          ╚═══════════╪════════════════╪═════════════════════════╪═══════════════════════════════════╪════════════════════════════════╪══════════════════════════════╪════════════════════════════╪════════════════════╝
                     HDFS            HBASE           ┌───────────┴────────────┐          ┌───────────┴───────────┐          ┌─────────┴──────────┐          ┌────────┴────────┐          ┌────────┴────────┐            
                    ,.-^^-._        ,.-^^-._         │HBaseCredentialsProvider│          │HdfsCredentialsProvider│          │[ApplicationMaster] │          │[Driver]         │          │[Executor]       │            
                   |-.____.-|      |-.____.-|        └────────────────────────┘          └───────────────────────┘          │AMCredentialsRenewer│          │CredentialUpdater│          │CredentialUpdater│            
                   |        |      |        |                                                                               └────────────────────┘          └─────────────────┘          └─────────────────┘            
                   |        |      |        |                                                                                                                                                                           
                   |        |      |        |                                                                                                                                                                           
                   '-.____.-'      '-.____.-'                                                                                                                                                                           

```


## Deploy wasp credential providers:

place `wasp-yarn-auth-hdfs.jar` into the directory pointed by `wasp.spark-streaming.yarn-jar`

place `wasp-yarn-auth-hbase.jar` into the directory pointed by `wasp.spark-streaming.yarn-jar`

place `wasp-yarn-auth-hdfs.jar` into the directory pointed by `wasp.spark-batch.yarn-jar`

place `wasp-yarn-auth-hbase.jar` into the directory pointed by `wasp.spark-batch.yarn-jar`


place under freeform configurations (`wasp.spark-batch.others` and `wasp.spark-streaming.others`)


```
 others = [
        #disable builtin hbase provider
	{"spark.yarn.security.credentials.hbase.enabled" = false} 
        #disable builtin hdfs provider
        {"spark.yarn.security.credentials.hadoopfs.enabled" = false} 
        #disable caching of FileSystem instances by hadoop code (it would cache expired tokens)
	{ "spark.hadoop.fs.hdfs.impl.disable.cache" = true} 
        #am needs to know the principal
	{ "spark.yarn.principal" = "andrea.fonti@CLUSTER01.ATSCOM.IT"}
        #am needs a keytab
        { "spark.yarn.keytab" = "andrea.fonti.2.keytab"}
        #how often am should check for renewal
	{ "spark.yarn.credentials.renewalTime" = "10000"}
        #how often executors and driver should check for renewal
        { "spark.yarn.credentials.updateTime" = "10000"}
        #distribute keytab
        { "spark.yarn.dist.files" = "file:///root/configurations/andrea.fonti.keytab" }
        #force spark to authenticate
        { "spark.authenticate" = "true" }
        #hadoop file system to access (pipe separated uris)
        { "spark.wasp.yarn.security.tokens.hdfs.fs.uris"="hdfs://nameservice1/user/andrea.fonti" }
        #place this if you want to debug the application master
#	{ "spark.yarn.am.extraJavaOptions" = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5008"}
```


## Credentials provider specific configurations

should be placed in the same free form block `others`

code is better than documentation here, sorry

```scala
object HbaseCredentialsProviderConfiguration {

  private val HADOOP_CONF_TO_LOAD_KEY = "spark.wasp.yarn.security.tokens.hbase.config.files"
  private val HADOOP_CONF_TO_LOAD_DEFAULT = ""
  private val HADOOP_CONF_TO_LOAD_SEPARATOR_KEY = "spark.wasp.yarn.security.tokens.hbase.config.separator"
  private val HADOOP_CONF_TO_LOAD_SEPARATOR_DEFAULT = "|"
  private val HADOOP_CONF_TO_LOAD_INLINE_PREFIX = "spark.wasp.yarn.security.tokens.hbase.config.inline"
  private val HADOOP_CONF_FAILFAST_KEY = "spark.wasp.yarn.security.tokens.hbase.failfast"
  private val HADOOP_CONF_FAILFAST_DEFAULT = true

  def fromSpark(conf: SparkConf): HbaseCredentialsProviderConfiguration = {

    val actualSeparator = conf.get(HADOOP_CONF_TO_LOAD_SEPARATOR_KEY, HADOOP_CONF_TO_LOAD_SEPARATOR_DEFAULT)

    val filesToLoad = conf.get(HADOOP_CONF_TO_LOAD_KEY, HADOOP_CONF_TO_LOAD_DEFAULT)
      .split(Pattern.quote(actualSeparator))
      .filterNot(_.isEmpty)
      .map(new Path(_))

    val other = conf.getAllWithPrefix(HADOOP_CONF_TO_LOAD_INLINE_PREFIX)

    val failFast = conf.getBoolean(HADOOP_CONF_FAILFAST_KEY, HADOOP_CONF_FAILFAST_DEFAULT)

    HbaseCredentialsProviderConfiguration(filesToLoad, failFast, other)
  }


  def toHbaseConf(conf: HbaseCredentialsProviderConfiguration): Configuration = {
    val hbaseConfig = HBaseConfiguration.create()

    conf.configurationFiles.foreach(hbaseConfig.addResource)
    conf.other.foreach {
      case (k, v) => hbaseConfig.set(k, v)
    }

    if (conf.failFast) {
      hbaseConfig.set("hbase.client.retries.number ", "1")
    }

    hbaseConfig
  }
}
```


```scala

object HdfsCredentialProviderConfiguration {

  private val KMS_SEPARATOR_KEY = "spark.wasp.yarn.security.tokens.hdfs.kms.separator"
  private val KMS_SEPARATOR_DEFAULT = "|"
  private val KMS_URIS_KEY = "spark.wasp.yarn.security.tokens.hdfs.kms.uris"
  private val KMS_URIS_VALUE = ""


  private val FS_SEPARATOR_KEY = "wasp.yarn.security.tokens.hdfs.fs.separator"
  private val FS_SEPARATOR_DEFAULT = "|"
  private val FS_URIS_KEY = "spark.wasp.yarn.security.tokens.hdfs.fs.uris"
  private val FS_URIS_VALUE = ""

  private val RENEW_KEY = "spark.wasp.yarn.security.tokens.hdfs.renew"
  private val RENEW_DEFAULT = 600000

  def fromSpark(conf: SparkConf): HdfsCredentialProviderConfiguration = {

    val kmsSeparator = conf.get(KMS_SEPARATOR_KEY, KMS_SEPARATOR_DEFAULT)

    val kmsUris = conf.get(KMS_URIS_KEY, KMS_URIS_VALUE)
      .split(Pattern.quote(kmsSeparator))
      .filterNot(_.isEmpty)
      .map(new URI(_))
      .toVector

    val fsSeparator = conf.get(FS_SEPARATOR_KEY, FS_SEPARATOR_DEFAULT)

    val fsUris = conf.get(FS_URIS_KEY, FS_URIS_VALUE)
      .split(Pattern.quote(fsSeparator))
      .filterNot(_.isEmpty)
      .map(new Path(_))
      .toVector

    val renew = conf.getLong(RENEW_KEY, RENEW_DEFAULT)

    HdfsCredentialProviderConfiguration(kmsUris, fsUris, renew)
  }

  def toSpark(conf: HdfsCredentialProviderConfiguration): SparkConf = {
    val c = new SparkConf()

    c.set(RENEW_KEY, conf.renew.toString)
    c.set(KMS_URIS_KEY, conf.kms.map(_.toString).mkString(KMS_SEPARATOR_DEFAULT))
    c.set(FS_URIS_KEY, conf.fs.map(_.toString).mkString(FS_URIS_VALUE))

  }
}
```


## Check if everything is working


Go to the application master log page and search for these logs

```

INFO  2019-03-01 18:35:37,375 o.a.s.d.y.s.AMCredentialRenewer: Credentials have expired, creating new ones now.
INFO  2019-03-01 18:35:37,376 o.a.s.d.y.s.AMCredentialRenewer: Attempting to login to KDC using principal: andrea.fonti@CLUSTER01.ATSCOM.IT
INFO  2019-03-01 18:35:37,378 o.a.s.d.y.s.AMCredentialRenewer: Successfully logged into KDC.
INFO  2019-03-01 18:35:38,165 i.a.b.w.y.a.h.HBaseCredentialsProvider: Provider config is: HbaseCredentialsProviderConfiguration(WrappedArray(),true,WrappedArray())
INFO  2019-03-01 18:35:38,594 i.a.b.w.y.a.h.HBaseCredentialsProvider: Token renewed Username=andrea.fonti@CLUSTER01.ATSCOM.IT, SequenceNumber=433, KeyId=15343, IssueDate=Fri Mar 01 18:35:38 CET 2019, ExpirationDate=Fri Mar 01 18:37:38 CET 2019
INFO  2019-03-01 18:35:38,594 i.a.b.w.y.a.h.HBaseCredentialsProvider: renewal of hbase token calculated from token info will happen before Fri Mar 01 18:37:38 CET 2019
INFO  2019-03-01 18:35:38,606 i.a.b.w.y.a.h.HdfsCredentialProvider: Provider config is: HdfsCredentialProviderConfiguration(Vector(),Vector(hdfs://nameservice1/user/andrea.fonti),130000)
INFO  2019-03-01 18:35:38,657 o.a.h.h.DFSClient: Created token for andrea.fonti: HDFS_DELEGATION_TOKEN owner=andrea.fonti@CLUSTER01.ATSCOM.IT, renewer=yarn, realUser=, issueDate=1551461738626, maxDate=1551461978626, sequenceNumber=11863, masterKeyId=15324 on ha-hdfs:nameservice1
INFO  2019-03-01 18:35:38,665 i.a.b.w.y.a.h.HdfsCredentialProvider: obtained HDFS delegation token Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:nameservice1, Ident: (token for andrea.fonti: HDFS_DELEGATION_TOKEN owner=andrea.fonti@CLUSTER01.ATSCOM.IT, renewer=yarn, realUser=, issueDate=1551461738626, maxDate=1551461978626, sequenceNumber=11863, masterKeyId=15324)
INFO  2019-03-01 18:35:38,668 i.a.b.w.y.a.h.HdfsCredentialProvider: Final renewal deadline will be Fri Mar 01 18:39:38 CET 2019
INFO  2019-03-01 18:35:38,678 o.a.s.d.y.s.AMCredentialRenewer: Writing out delegation tokens to hdfs://nameservice1/user/andrea.fonti/.sparkStaging/application_1551348340529_0023/credentials-e5ad77a1-b400-443c-91ca-2826a7c2d031-1551461834572-1.tmp
INFO  2019-03-01 18:35:38,849 o.a.s.d.y.s.AMCredentialRenewer: Delegation Tokens written out successfully. Renaming file to hdfs://nameservice1/user/andrea.fonti/.sparkStaging/application_1551348340529_0023/credentials-e5ad77a1-b400-443c-91ca-2826a7c2d031-1551461834572-1
INFO  2019-03-01 18:35:38,866 o.a.s.d.y.s.AMCredentialRenewer: Delegation token file rename complete.
INFO  2019-03-01 18:35:38,881 o.a.s.d.y.s.AMCredentialRenewer: Scheduling login from keytab in 89697 millis.
```

Check logs of spark-consumers-streaming:

verify thet scheduled refresh is plausible

```
INFO  2019-03-01 17:36:40,195 o.a.s.d.y.s.CredentialUpdater: Reading new credentials from hdfs://nameservice1/user/andrea.fonti/.sparkStaging/application_1551348340529_0023/credentials-e5ad77a1-b400-443c-91ca-2826a7c2d031-1551461834572-1
INFO  2019-03-01 17:36:40,239 o.a.s.d.y.s.CredentialUpdater: Credentials updated from credentials file.
INFO  2019-03-01 17:36:40,239 o.a.s.d.y.s.CredentialUpdater: Scheduling credentials refresh from HDFS in 34333 ms.
INFO  2019-03-01 17:37:14,613 o.a.s.d.y.s.CredentialUpdater: Reading new credentials from hdfs://nameservice1/user/andrea.fonti/.sparkStaging/application_1551348340529_0023/credentials-e5ad77a1-b400-443c-91ca-2826a7c2d031-1551461924682-2
```

verify that the watchdog does not report anomalies

```
INFO  2019-03-01 18:33:05,927 i.a.b.w.c.s.s.a.w.SparkContextWatchDog: Everything is fine, delegation tokens are ok
INFO  2019-03-01 18:33:06,927 i.a.b.w.c.s.s.a.w.SparkContextWatchDog: all token identifiers : List(org.apache.hadoop.yarn.security.AMRMTokenIdentifier@6a32009d, org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier@1d8, token for andrea.fonti: HDFS_DELEGATION_TOKEN owner=andrea.fonti@CLUSTER01.ATSCOM.IT, renewer=yarn, realUser=, issueDate=1551465162963, maxDate=1551465402963, sequenceNumber=11974, masterKeyId=15352)
INFO  2019-03-01 18:33:06,927 i.a.b.w.c.s.s.a.w.SparkContextWatchDog: filtered token identifiers : List(token for andrea.fonti: HDFS_DELEGATION_TOKEN owner=andrea.fonti@CLUSTER01.ATSCOM.IT, renewer=yarn, realUser=, issueDate=1551465162963, maxDate=1551465402963, sequenceNumber=11974, masterKeyId=15352)
INFO  2019-03-01 18:33:06,927 i.a.b.w.c.s.s.a.w.SparkContextWatchDog: Expired tokens? : None
```

check logs of executors
```
INFO  2019-03-01 17:36:40,195 o.a.s.d.y.s.CredentialUpdater: Reading new credentials from hdfs://nameservice1/user/andrea.fonti/.sparkStaging/application_1551348340529_0023/credentials-e5ad77a1-b400-443c-91ca-2826a7c2d031-1551461834572-1
INFO  2019-03-01 17:36:40,239 o.a.s.d.y.s.CredentialUpdater: Credentials updated from credentials file.
INFO  2019-03-01 17:36:40,239 o.a.s.d.y.s.CredentialUpdater: Scheduling credentials refresh from HDFS in 34333 ms.
INFO  2019-03-01 17:37:14,613 o.a.s.d.y.s.CredentialUpdater: Reading new credentials from hdfs://nameservice1/user/andrea.fonti/.sparkStaging/application_1551348340529_0023/credentials-e5ad77a1-b400-443c-91ca-2826a7c2d031-1551461924682-2
```

# WASP 2.19.5

Minor fixes

# WASP 2.19.6

### Perform avro encoding inside spark expression via code generation 3

[Merge request 130](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/130)

Created at: 2019-03-28T18:10:44.426Z

Updated at: 2019-04-01T17:00:49.467Z

Branch: feature/206-perform-avro-encoding-inside-spark-expression-via-code-generation-3

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Avro serialization is now performed in a spark sql expression,
the sql expression will use the spark codegen creating less serialization boundaries,
the spark plan will be easier to optimize for the planner.

Various optimization were performed in avro schema handling and serialization exploiting the schema registry caching mechanism to perform less round trips of json schemas (that should be parsed from json, an expensive operation)

Thnx to @mark91  and @antonio.murgia

### Resolve "Pipegraph high availability will sometimes get confused"

[Merge request 131](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/131)

Created at: 2019-04-01T15:11:19.548Z

Updated at: 2019-04-01T17:06:15.371Z

Branch: feature/209-pipegraph-high-availability-will-sometimes-get-confused

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Closes #209

## WASP 2.19.7

### Spark session should be cloned for each etl to prevent global options to affect different strategies in non deterministic order

[Merge request 134](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/134)

Created at: 2019-04-02T13:29:50.281Z

Updated at: 2019-04-02T16:10:45.347Z

Branch: feature/210-spark-session-should-be-cloned-for-each-etl-to-prevent-global-options-to-affect-different-strategies-in-non-deterministic-order

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Structured streaming etls now do not share Spark Sessions.

Closes #210

### Let user configure the name of the telemetry topic

[Merge request 135](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/135)

Created at: 2019-04-02T16:12:20.943Z

Updated at: 2019-04-10T14:27:49.883Z

Branch: feature/205-let-user-configure-the-name-of-the-telemetry-topic

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Closes #205 

Telemetry topic name is now configurable, one producer per process instead of one producer per ETL is instantiated

Telemetry is now configurable via the following HOCON configuration


```
wasp {
    telemetry{
        writer = "default"
        latency.sample-one-message-every = 100
        topic = {
        name = "telemetry"
        partitions = 3
        replica = 1
        others = [
            {"batch.size" = "1048576"}
            {"acks" = "0" }
        ]
        }
    }
}

```


`wasp.telemetry.writer`: can be "solr", "elastic" or "default"

`wasp.telemetry.latency.sample-one-message-every`: affect sampling of messages in end to end message latency telemetry

`wasp.telemetry.topic.name`: name of the telemetry topic to write to `.topic` will be appended to it by wasp

`wasp.telemetry.topic.partitions`: number of partitions of topic to create if topic is created by wasp

`wasp.telemetry.topic.replica`: number of replica of topic if topic is created by wasp

`wasp.telemetry.topic.others`: list of free form tuples (string, string) that will be appended to the kafka telemetry producer configuration after the global options set by `wasp.kafka.others`

relevant code describing how configurations of KafkaProducer for telemetry are set

```scala
    val kafkaConfig = ConfigManager.getKafkaConfig

    val telemetryConfig = ConfigManager.getTelemetryConfig

    val connectionString = kafkaConfig.connections.map {
      conn => s"${conn.host}:${conn.port}"
    }.mkString(",")


    val props = new Properties()
    props.put("bootstrap.servers", connectionString)
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val notOverridableKeys = props.keySet.asScala

    val merged: Seq[KafkaEntryConfig] = kafkaConfig.others ++ telemetryConfig.telemetryTopicConfigModel.kafkaSettings

    val resultingConf = merged.filterNot(x => notOverridableKeys.contains(x.key))

    logger.info(s"Telemetry configuration\n${resultingConf.mkString("\n")}" )

    resultingConf.foreach {
      case KafkaEntryConfig(key, value) => props.put(key, value)
    }

```

### Let users configure compression on a topic-by-topic basis

[Merge request 141](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/141)

Created at: 2019-04-10T14:25:56.970Z

Updated at: 2019-04-11T13:46:06.005Z

Branch: feature/217-let-users-configure-compression-on-a-topic-by-topic-basis

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

Closes #217 


TopicModel now has a new field called topicCompression, it's default value is `TopicCompression.Disabled`


To use compression the relevant TopicModels should be updated.


Available compression methods are

```scala
TopicCompression.Disabled // No compression will be used by the producer,
TopicCompression.Gzip // Gzip compression will be applied by the producer for each sent batch (High CPU usage best compression),
TopicCompression.Snappy // "snappy" (Medium CPU usage lowest compression),
TopicCompression.Lz4 // "lz4" (Lower CPU usage than snappy, slightly better compression than snappy)
```

For convenience an overview of different compression algorithms is available in the following image

![Rplot05](/uploads/00dbaa4abda925effc6bd2172b1196b8/Rplot05.png)

## WASP 2.19.8

### Optimize avro conversions writing to hbase

[Merge request 137](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/137)

Created at: 2019-04-08T08:27:47.268Z

Updated at: 2019-04-11T16:21:49.287Z

Branch: feature/208-optimize-avro-conversions-writing-to-hbase

Author: [Marco](https://gitlab.com/mark91)

The PR leverages the `AvroConverterExpression` when writing to HBase and removes `AvroToRow`: this provides great perf improvements when writing to HBase.

Thanks to @antonio.murgia for the great help on this.

cc @andrea.fonti

### Use native expression in order to convert avro from kafka

[Merge request 139](https://gitlab.com/AgileFactory/Agile.Wasp2/merge_requests/139)

Created at: 2019-04-09T08:46:44.745Z

Updated at: 2019-04-12T08:22:21.543Z

Branch: feature/215-use-native-expression-in-order-to-convert-avro-from-kafka-2

Author: [Marco](https://gitlab.com/mark91)

Closes #215

Improves performance reading avro serialized data from kafka


## WASP 2.20.0

### [#234] Add a way to start singletons services inside executors, develop a jmx scraper as a poc

    commit c4e92fb2ec5cb03b380dba15de626593decb4e6a
    Author: Andrea Fonti <andrea.fonti@agilelab.it>
    Date:   Thu Jul 25 16:44:41 2019 +0000
    
#### New features and improvements

##### New Whitelabel based on a standalone docker container

Hadoop services and wasp now run in a single container, the wasp deployment is based on supervisord.

##### New spark executor plugin

`it.agilelab.bigdata.wasp.spark.plugins.telemetry.TelemetryPlugin`

To enable the telemetry plugin you need to put the relevant jars on hdfs in `spark-yarn-jar` dir

```
hdfs dfs -copyFromLocal $WASP_LIB/it.agilelab.wasp-spark-telemetry-plugin-*.jar $SPARK_YARN_JAR_DIR
hdfs dfs -copyFromLocal $WASP_LIB/org.apache.kafka.kafka-clients-0.11.0-kafka-3.0.0.jar $SPARK_YARN_JAR_DIR
```

and add an extra spark option to the streaming or batch configuration

```json
wasp.spark-streaming.others : [
        {"spark.executor.plugins": "it.agilelab.bigdata.wasp.spark.plugins.telemetry.TelemetryPlugin"}
        { "spark.executor.extraJavaOptions" : "-Dwasp.plugin.telemetry.collection-interval=\"1 second\"" }
    ]
```

or

```json
wasp.spark-batch.others : [
        {"spark.executor.plugins": "it.agilelab.bigdata.wasp.spark.plugins.telemetry.TelemetryPlugin"}
        { "spark.executor.extraJavaOptions" : "-Dwasp.plugin.telemetry.collection-interval=\"1 second\"" }
    ]
```

To configure which java mbeans are scraped for telemetry you need to set configuration like this:

```json
wasp.telemetry.topic.jmx = [
        {
            query = "java.lang:*"
            metricGroupAttribute = "name"
            sourceIdAttribute = "unknown"
            metricGroupFallback = "jvm"
            sourceIdFallback = "jvm"
        },
        {
            query = "kafka.producer:*"
            metricGroupAttribute = "type"
            sourceIdAttribute = "client-id"
        },
        {
            query = "kafka.consumer:*"
            metricGroupAttribute = "type"
            sourceIdAttribute="client-id"
        },
        {
            query = "\"org.apache.hadoop.hbase.client\":*"
            metricGroupAttribute ="name"
            sourceIdAttribute="scope"
        }
        ]
```

telemetry will be scraped from jmx and sent to the telemetry topic in this format

```json
{
    "timestamp":"2019-07-25T09:40:57.903Z",
    "tag":"1",
    "metric":"org.apache.hadoop.hbase.client.rpc-call-duration-ms-client-service-mutate-increment.50th-percentile",
    "messageId":"b141091e-5b93-458b-b7ee-905c693c5f7e",
    "value":0.0,
    "sourceId":"hconnection-0x6d1e30f7"
}
```

for example, the configuration specifies `sourceIdAttribute=scope`

`sourceId` of the telemetry message was derived by the jmx key `scope` of the bean identified by the query  `\"org.apache.hadoop.hbase.client\":*` (yes the quotes are included in the query, hbase registers the bean like that)

the prefix of the `metric` field was derived from the query `org.apache.hadoop.hbase.client` the suffix was derived by concatenating the result of the key `name` specified via `metricGroupAttribute="scope"` and the bean attribute name

#### Breaking changes

No breaking changes

#### Migration

To use the feature please perform all the steps to enable the executor plugin

#### Bug fixes

No bugs fixed

#### Related issue

Closes #234



### [#202] Don't instantiate one producer per partition in end to end message latency telemetry

    commit 12a3633491ec5b86fe9bd2a9c5f3d852bfdff962
    Author: Andrea Latella <andrea.latella87@gmail.com>
    Date:   Thu Jul 18 15:08:42 2019 +0000

#### New features and improvements

Now one kafka producer will be spawned per executor

if you need to send additional telemetry:

1. on the driver side get hold of the configuration needed

    ```
    val config = TelemetryMetadataProducerConfig(ConfigManager.getKafkaConfig.toTinyConfig(),
                    ConfigManager.getTelemetryConfig)
    ```

2. invoke the method `it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.TelemetryMetadataProducer#send`

    In order to provide a JsValue taske a look at `it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MetricsTelemetryMessageFormat$#metricsTelemetryMessageFormat`

#### Breaking changes

No breaking changes

#### Migration

No migration required

#### Bug fixes

Now telemetry producers are shared executor side

#### Related issue

Closes #202


## WASP 2.21.0

### feature: Wasp could expose before and after materialize hooks

[Merge request 152](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/152)

Created at: 2019-10-31T16:20:17.020Z

Updated at: 2020-02-28T16:02:43.448Z

Branch: feature/237-wasp-could-expose-before-and-after-materialize-hooks

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

#### New features and improvements

Strategies can now mix also this trait

```scala
/**
  * Mixin this trait to a strategy that needs an hook after the write phase,
  * the framework will detect this trait and call postMaterialization after the relevant phase
  */
trait HasPostMaterializationHook {
  self: Strategy =>

  /**
    *
    * @param maybeDataframe (optional) output dataframe of the strategy (it is None only if the strategy has no writer)
    * @param maybeError     Some(error) if an error happened during materialization; None otherwise
    * @return the wanted outcome of the strategy. This means that if the maybeError is not None and the hook does not
    *         have a "recovery" logic, it should always return a Failure wrapping the input error.
    */
  def postMaterialization(maybeDataframe: Option[DataFrame], maybeError: Option[Throwable]): Try[Unit]


}
```

To declare that they need a hook after materialization of batch a job to do cleanup operations, strategies that do not mix this trait will work as usual

#### Breaking changes

No breaking changes

#### Migration

No migration required

#### Bug fixes

No bugs fixed

#### Related issue

Closes #237

### feature: Create spark plugin that writes to mongodb

[Merge request 153](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/153)

Created at: 2020-01-16T17:46:29.678Z

Updated at: 2020-02-28T16:02:44.036Z

Branch: feature/239-create-spark-plugin-that-writes-to-mongodb

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

#### New features and improvements

New Mongo db plugin

Supports:

* Streaming writes to collection
* Batch reads from collection
* Batch writes from collection


See documentation of spark-mongo for configuration keys

https://docs.mongodb.com/spark-connector/v2.2/

those configuration keys should be added in the ReaderModel and in the WriterModel


to demo the functionality via white-label project do the following:

```bash
cd whitelabel/docker-new/
./start-wasp.sh -d
http POST http://andreaf-laptop:2891/producers/TestJSONProducer/start
http POST http://andreaf-laptop:2891/pipegraphs/TestMongoWriterStructuredJSONPipegraph/start
http POST http://andreaf-laptop:2891/pipegraphs/TestMongoWriterStructuredJSONPipegraph/stop
http POST http://andreaf-laptop:2891/batchjobs/TestBatchJobFromMongoNestedToConsole/start
http POST http://andreaf-laptop:2891/pipegraphs/TestHdfsWriterStructuredJSONPipegraph/start
http POST http://andreaf-laptop:2891/pipegraphs/TestHdfsWriterStructuredJSONPipegraph/stop
http POST http://andreaf-laptop:2891/batchjobs/TestBatchJobFromHdfsNestedToMongo/start
http POST http://andreaf-laptop:2891/producers/TestJSONProducer/stop
```

example models

```scala
writeToMongo = DocumentModel(
    name = "test-write-to-mongo",
    connectionString = ConfigManager.getMongoDBConfig.address + "/" +  ConfigManager.getMongoDBConfig.databaseName + "." + "TestCollectionStructuredWriteMongo",
    schema = StructType(Seq(
      StructField("id", StringType),
      StructField("number", IntegerType),
      StructField("nested", StructType(Seq(
        StructField("field1", StringType),
        StructField("field2", LongType),
        StructField("field3", StringType)
      )))
    )).json
  )
```

```scala
lazy val mongo = PipegraphModel(
        name = "TestMongoWriterStructuredJSONPipegraph",
        description = "Description of TestMongoWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestConsoleWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.mongoDbWriter("Mongo Writer", TestMongoModel.writeToMongo, Map.empty),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )
```

```scala
 lazy val nestedToMongo = BatchJobModel(
      name = "TestBatchJobFromHdfsNestedToMongo",
      description = "Description of TestBatchJobFromHdfsNestedToConsole",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFromHdfsNestedToConsole",
        inputs = List(
          ReaderModel.rawReader("TestRawNestedSchemaModel", TestRawModel.nested)
        ),
        output = WriterModel.mongoDbWriter("Console Writer", TestMongoModel.writeToMongo, Map.empty),
        mlModels = List(),
        strategy = Some(StrategyModel.create("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestIdentityStrategy",
          ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1"""))),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      )
    )
```

```scala
/**
      *  Fail if the HDFS directory does not exist
      */
    lazy val nestedToConsole = BatchJobModel(
      name = "TestBatchJobFromMongoNestedToConsole",
      description = "Description of TestBatchJobFromHdfsNestedToConsole",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFromHdfsNestedToConsole",
        inputs = List(
          ReaderModel.mongoDbReader("TestRawNestedSchemaModel", TestMongoModel.writeToMongo, Map.empty)
        ),
        output = WriterModel.consoleWriter("Console Writer"),
        mlModels = List(),
        strategy = Some(StrategyModel.create("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestIdentityStrategy",
          ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1"""))),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      )
    )
```

#### Breaking changes

None

#### Migration

None

#### Bug fixes

None

#### Related issue

Closes #239

### feature: Implement AvroExpressionEncoder

[Merge request 156](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/156)

Created at: 2020-02-01T15:16:48.600Z

Updated at: 2020-02-28T16:02:44.412Z

Branch: feature/248-implement-avroexpressionencoder

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

#### New features and improvements


As of now when anyone develops a Spark Structured Streaming stateful query (this is not limited to Wasp, even if, Wasp puts structured streaming at its core), has to choose encoders for both it's ouput dataframe and the state he wants to keep. Looking at flatMapGroupsWithState signature:

```scala
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U]
```

`Encoder[U]` is important for downstream computations but it's never persisted anywhere (except for shuffles and dataframe persistence (i.e. caching)), unless obviously it's directed to the "sink" of the streaming query and explicitly written somewhere explicitly.

`Encoder[S]` is instead persisted by the `StateStore` implementation of choice, which is almost every time, `HDFSBackedStateStore`. This means that if an application evolves and its state evolves with it, there is need for the Encoder to support some kind of schema evolution.

 The current only available choices are for `Encoder[_]`:
- `Encoders.product`: which encodes the case class as a "well-typed" spark Row (i.e. each field of the case class is a "column" with its spark sql type and metadata attached). The row serialization is very naive and straightforward: every column is transformed into raw bytes and appended, it's mandatory to know the schema to read it back, there is no support to schema evolution whatsoever. If it happens that you append fields, it might work and you will have null values, bettere than nothing, but certainly not an intended feature I would like to rely on.
- `Encoders.kryo`: which relies upon [Kryo serialization framework](https://github.com/EsotericSoftware/kryo/blob/master/README.md) to create a Spark row with only one column name "value" and of type `Array[Byte]`. This approach has downsides and upsides: having a single column makes the Spark optimizer work worthless but much much faster, the downside is that you cannot apply any sql function to the byte array. This downside is not much of a problem, because the only stateful aggregations that are possible to perform are "typed" i.e. rely on the user to provide an arbitrary scala function that works on JVM objects (i.e. there cannot be use of sql statements and codegen optimization a-priori). Unfortunately it is not clear (to me, at least) how Kryo supports schema evolution. Moreover being based on Java classes, moving the class to a different package (for example) would cause problems.

This pull request implements Avro based Encoders, sample usage is

```scala
 "correctly handle serialization when not using darwin" in testAllCodegen {
      val elements = generateData(4, 1000)
      manager.registerAll(schema1 :: Nil)
      val encoder = AvroEncoders.avroEncoder(
        schema1,
        () => manager,
        toRecord1,
        fromRecord1
      )
      val df = ss.createDataset(elements)(encoder)

      assert(df.schema.fieldNames.toList == Seq("value"))
      val result = df.collect()
      assert(result sameElements elements)
    }
```

#### Breaking changes

None

#### Migration

None

#### Bug fixes

None

#### Related issue

Closes #248

### feature: AvroSerDeExpressions cannot deal with Map[String, A]

[Merge request 158](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/158)

Created at: 2020-02-04T10:58:24.047Z

Updated at: 2020-02-28T16:02:44.747Z

Branch: feature/250-avroserdeexpressions-cannot-deal-with-map-string-a

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

Assignee: [Andrea Fonti](https://gitlab.com/andrea.fonti)

#### New features and improvements

`Map[String,A]` where a `A` is a compatible avro <-> catalyst type are now encoded and decoded correctly.

#### Breaking changes

None.

#### Migration

None.

#### Bug fixes

`Map[String,A]` where a `A` is a compatible avro <-> catalyst type are now encoded and decoded correctly.

#### Related issue

Closes #250

### feature: Docker whitelabel on CDH7

[Merge request 160](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/160)

Created at: 2020-02-24T10:49:10.657Z

Updated at: 2020-02-28T16:02:45.108Z

Branch: feature/263-docker-whitelabel-on-cdh7

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

#### New features and improvements

##### Docker CDH 7 Image

###### Installed  services

* Zookeeper
* Hbase master
* Hbase region server
* Hadoop Name Node
* Hadoop Data Node
* Solr
* Mongodb
* Spark2
* Kafka


###### Run wasp inside cdh7 container

```
cd Agile.Wasp2/whitelabel/cdh7
./start-wasp.sh
```

###### Run the container standalone

```
cd Agile.Wasp2/whitelabel/cdh7/worker
docker-compose run --service-ports cdh
```
###### Web UI
 

- *60010* Hbase Master
- *60020* Hbase Region server
- *8088*  Yarn Resource manager
- *8042* Yarn node manager
- *8983*  Solr
- *27017* Mongodb

###### How to build

```
cd Agile.Wasp2/whitelabel/cdh7
bash build-and-push.sh
```

You will be asked for your gitlab username and password, if you want to perform a local build use the following commmand

```
cd base
cd cache
wget https://archive.cloudera.com/cdh7/7.0.3.0/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019-el7.parcel
mv CDH-7.0.3-1.cdh7.0.3.p0.1635019-el7.parcel CDH-7.0.3-1.cdh7.0.3.p0.1635019-el7.parcel.tar.gz
cd ..
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdh7:base
cd ..
cd worker
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdh7:worker
```

##### Caveats

* Data directories of services are not exported as volumes, removing the container will delete all data stored inside services, you can always stop and restart the container without losing data


#### Breaking changes

No breaking changes

#### Migration

No migration required

#### Bug fixes

No bug fixes

#### Related issue

Closes #263 

### feature: PipegraphGuardian logs at warn level every unhandled message

[Merge request 161](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/161)

Created at: 2020-02-27T15:45:29.261Z

Updated at: 2020-02-28T16:02:45.436Z

Branch: feature/228-pipegraphguardian-logs-at-warn-level-every-unhandled-message

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

Assignee: [Andrea Fonti](https://gitlab.com/andrea.fonti)

#### New features and improvements

* Minor usability improvements, logs are less spammy

#### Breaking changes

None

#### Migration

None

#### Bug fixes

None

#### Related issue

Closes #228

## WASP 2.22.0

### Resolve "GDPR - Data Deletion"

[Merge request 159](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/159)

Created at: 2020-02-14T16:46:25.361Z

Updated at: 2020-03-19T09:57:56.742Z

Branch: feature/246-gdpr-data-deletion

Author: [Giuseppe Lillo](https://gitlab.com/giuseppe.lillo)

## GDPR deletion feature inside Wasp

This functionality will be provided as a special Wasp BatchJob.

### `BatchGdprETLModel`

A Batch job ETL specialized for data deletion will be available:

```scala
sealed trait BatchETL {
  val name: String
  val inputs: List[ReaderModel]
  val output: WriterModel
  val group: String
  var isActive: Boolean
}

case class BatchETLModel(name: String,
                         inputs: List[ReaderModel],
                         output: WriterModel,
                         mlModels: List[MlModelOnlyInfo],
                         strategy: Option[StrategyModel],
                         kafkaAccessType: String,
                         group: String = "default",
                         var isActive: Boolean = false) extends BatchETL

case class BatchGdprETLModel(name: String,
                             dataStores: List[DataStoreConf],
                             strategyConfig: Config,
                             inputs: List[ReaderModel],
                             output: WriterModel,
                             group: String,
                             var isActive: Boolean = false) extends BatchETL {
  val strategy: GdprStrategyModel = GdprStrategyModel.create(
    "it.agilelab.bigdata.wasp.consumers.spark.strategies.GdprStrategy",
    dataStores,
    strategyConfig
  )
}


case class GdprStrategyModel(className: String,
                             dataStoresConf: List[DataStoreConf],
                             configuration: Option[String] = None) {
  def configurationConfig(): Option[Config] = configuration.map(ConfigFactory.parseString)
}

object GdprStrategyModel {
  def create(className: String,
             dataStores: List[DataStoreConf],
             configuration: Config): GdprStrategyModel = {
    GdprStrategyModel(className, dataStores, Some(configuration.root().render(ConfigRenderOptions.concise())))
  }
}
```

`BatchGdprETLModel` contains the following informations:

- `inputs: List[ReaderModel]` : these `ReaderModel`s refer to the data that contain the list of keys to be "cleaned".
  - Example: a RawModel that points to a HDFS file containing all the keys that need to be "cleaned".
- `dataStores: List[DataStoreConf]` : a list of DataStores where the data to be deleted is stored. These DataStores can be of two different types: KeyValue (HBase) or RawData (Parquet). For each of those types, there can be different strategies for finding the data to delete, encoded as an ADT:

```scala
sealed trait DataStoreConf {
  final val storage: String = this.getClass.getSimpleName
}

final case class KeyValueDataStoreConf(
                                        keyValueModel: KeyValueModel, // here we retrieve tablename and namespace
                                        keyMatchingStrategy: KeyValueMatchingStrategy
                                      ) extends DataStoreConf
final case class RawDataStoreConf(
                                   rawModel: RawModel, // here we retrieve location, schema, format and partitioning
                                   rawMatchingStrategy: RawMatchingStrategy,
                                   partitionPruningStrategy: PartitionPruningStrategy
                                 ) extends DataStoreConf

sealed trait KeyValueMatchingStrategy
final case class ExactKeyValueMatchingStrategy(colName: String)  extends KeyValueMatchingStrategy
final case class PrefixKeyValueMatchingStrategy(colName: String) extends KeyValueMatchingStrategy
final case class PrefixAndTimeBoundKeyValueMatchingStrategy(colName: String, separator: String, format: DateFormat)
  extends KeyValueMatchingStrategy

sealed trait PartitionPruningStrategy
final case class TimeBasedBetweenPartitionPruningStrategy(colName: String, dateFormat: DateFormat)
  extends PartitionPruningStrategy
case object NoPartitionPruningStrategy extends PartitionPruningStrategy


sealed trait RawMatchingStrategy
case class ExactRawMatchingStrategy(dataframeKeyColName: String, inputKeyColName: String) extends RawMatchingStrategy
case class PrefixRawMatchingStrategy(dataframeKeyColName: String, inputKeyColName: String) extends RawMatchingStrategy
```
- `output: WriterModel` : describes how and where to write the result of the job, that will be a list of keys and whether the deletion was successful or not.

### `GdprStrategy`

The heavy lifting of this job will be done inside the `GdprStrategy`.

```scala
class GdprStrategy(dataStores: List[DataStoreConf]) extends Strategy {
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    // for each DataStore delete appropriate records
    ???
  }
}
```

This Strategy has the responsability to read the files according to the `DataStoreConf`s that it receives, delete them, and then return a DataFrame containing all the key processed, together with the result of the operation.

The REST configuration provided to the batch job when it's called will fill the configurations that are not specified inside the `DataStoreConf`, for example the start and end timestamp of the `TimeBasedBetweenPartitionPruningStrategy`

### `BatchJobActor`

In this way, the `BatchJobModel` can have two types of batch ETLs:

```scala
case class BatchJobModel(override val name: String,
                         description: String,
                         owner: String,
                         system: Boolean,
                         creationTime: Long,
                         etl: BatchETL, // BatchETLModel or BatchGdprETLModel
                         exclusivityConfig: BatchJobExclusionConfig = BatchJobExclusionConfig(
                           isFullyExclusive = true,
                           Seq.empty[String])
                        )
  extends Model
```

This will be handled inside the `BatchJobActor.run` method, in particular only in the function that creates the strategy.

---

This design guarantees that all the main models already defined by Wasp will not be altered, and the user will only need to define a new kind of BatchJob, including the information about the structure and the location of the data to be deleted.

## Deletion in details

### HDFS

In order to delete HDFS files the following steps are executed:

1. All the files are scanned in order to identify the ones that match the key(s) to be deleted
1. The files containing records to be deleted are read and rewritten (without the records to be deleted) to a staging directory
1. The new files overwrite the "old" files
1. The backup directory is then deleted

### HBase

In order to delete HBase rows the following steps are executed:

1. All the tables are scanned in order to identify the keys that must be deleted
1. Each key is then deleted

## Calling the job

OpenAPI model definition:

```
GdprStrategy:
    type: "object"
    properties:
      runId:
        type: "string"
        description: "unique identifier of a deletion job"
        default: "random UUID"
      outputPartitions:
        type: "integer"
        description: "number of partitions of output DataFrame"
        default: 1
      hdfs:
        type: "object"
        properties:
          keys:
            type: "array"
            description: "keys to delete. If set these keys will overwrite the keys read from the input model."
            items:
              type: "string"
            example: ["k1", "k2"]
          stagingDir:
            type: "string"
            description: "path to the staging directory where to save temporarly the newly created files"
            default: "RawModel.uri + '/staging'"
            example: "hdfs://09ae5edb8dab:9000/user/root/staging"
          backupDir:
            type: "string"
            description: "path to the directory where the backup directory to store the backup of the files to change will be created"
            default: "parent of RawModel.uri"
            example: "hdfs://09ae5edb8dab:9000/user/root/backups"
          start:
            type: "number"
            format: "int64"
            description: "start date of range to select in milliseconds"
            example: 1570733160000
          end:
            type: "number"
            format: "int64"
            description: "end date of range to select in milliseconds"
            example: 1571349600000
          timeZone:
            type: "string"
            description: "time zone of the date to consider"
            default: "UTC"
            example: "IT"
      hbase:
        type: "object"
        properties:
          keys:
            type: "array"
            description: "keys to delete. If set these keys will overwrite the keys read from the input model."
            items:
              type: "string"
            example: ["k1", "k2"]
          start:
            type: "number"
            format: "int64"
            description: "start date of range to select in milliseconds"
            example: 1570733160000
          end:
            type: "number"
            format: "int64"
            description: "end date of range to select in milliseconds"
            example: 1571349600000
          timeZone:
            type: "string"
            description: "time zone of the date to consider"
            default: "UTC"
            example: "IT"
```

Example:
```json
{
    "runId": "201910122046-run1",
    "outputPartitions": 5,
    "hbase": {
        "start": 1570733160000,
        "end": 1571349600000,
        "timeZone": "UTC"
    },
    "hdfs": {
        "start": 1570733160000,
        "end": 1571349600000,
        "timeZone": "UTC",
        "backupDir": "hdfs://09ae5edb8dab:9000/user/root/"
    }
}
```


### Resolve "Implement KafkaSparkBatchWriter"

[Merge request 164](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/164)

Created at: 2020-03-11T07:44:08.232Z

Updated at: 2020-03-19T09:57:57.095Z

Branch: feature/251-implement-kafkasparkbatchwriter

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

Assignee: [Nicolò Bidotti](https://gitlab.com/m1lt0n)

Closes #251

### Resolve "GDPR keys to delete should have a correlation id"

[Merge request 165](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/165)

Created at: 2020-03-16T15:36:02.682Z

Updated at: 2020-03-19T09:57:57.355Z

Branch: feature/277-gdpr-keys-to-delete-should-have-a-correlation-id

Author: [Giuseppe Lillo](https://gitlab.com/giuseppe.lillo)

Closes #277

## Wasp 2.23.0

### feature: Implement KafkaBacklogAnalyzer

[Merge request 168](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/168)

Created at: 2020-03-17T18:28:56.004Z

Updated at: 2020-03-30T10:41:28.460Z

Branch: feature/279-implement-kafkabackloganalyzer

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

Assignee: [Andrea Fonti](https://gitlab.com/andrea.fonti)

#### New features and improvements

A new abstract producer that is able to check the backlog size of an Spark Structured Streaming ETL.



#### Kafka metrics producers

We need to produce a suite of actors able to monitor the metrics of Kafka topics, there are mainly two needs that must be addressed:

- backlog of a list of pipegraphs or etls
- traffic of a list of topics

#### Backlog size calculator

In order to calculate the backlog of an ETL, the following information are needed:

- the latest offsets consumed by an ETL (provided by TelemetryActor)
- the latest offsets available in a Kafka topic (provided by KafkaActor)

#### Traffic calculator

In order to calculate the traffic of a kafka topic the following information are needed:

- the latest offsets available in a Kafka topic at the beginning of the time window (provided by KafkaActor)
- the latest offsets available in a Kafka topic at the end of the time window (provided by KafkaActor)

#### Kafka Actor

Actor tied with a single kafka topic which has the only duty to return the current latest offsets of a Kafka Topic.# Breaking changes

None.

#### Migration

None.

#### Bug fixes

None.

#### Related issue

Closes #279
Closes #236

## WASP 2.24.0

### Resolve "Remove the detail of all keys to be deleted from HDFS GDPR deletion"

[Merge request 174](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/174)

Created at: 2020-04-01T07:16:58.517Z

Updated at: 2020-04-01T17:40:00.187Z

Branch: feature/288-remove-the-detail-of-all-keys-to-be-deleted-from-hdfs-gdpr-deletion

Author: [Giuseppe Lillo](https://gitlab.com/giuseppe.lillo)

Assignee: [Antonio Murgia](https://gitlab.com/antonio.murgia)

Closes #288

### Resolve "Add a HdfsContainsColumnMatch for GDPR deletion job"

[Merge request 175](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/175)

Created at: 2020-04-01T13:06:37.479Z

Updated at: 2020-04-01T19:12:13.369Z

Branch: feature/289-allow-a-containsrawmatchingstrategy-for-gdpr-deletion-job

Author: [Giuseppe Lillo](https://gitlab.com/giuseppe.lillo)

Assignee: [Antonio Murgia](https://gitlab.com/antonio.murgia)

# New features and improvements

Add new `HdfsMatchType` for GDPRJob named `HdfsContainsColumnMatch` which matches if the selected field contains the key string.

# Breaking changes

None.

# Migration

None.

# Bug fixes

Remove a `dataframe.collect` that was made before re-writing an entire datastore.

# Related issue

Closes #289

### feature: HdfsDeletionHandler assumes data is in parquet format

[Merge request 176](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/176)

Created at: 2020-04-01T19:38:16.945Z

Updated at: 2020-04-15T16:37:53.890Z

Branch: feature/290-hdfsdeletionhandler-assumes-data-is-in-parquet-format

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

Assignee: [Giuseppe Lillo](https://gitlab.com/giuseppe.lillo)

# New features and improvements

GDPRDeletion can be applied to any structured raw model.

# Breaking changes

None.

# Migration

None.

# Bug fixes

- GDPRDeletion now works with any structured raw model and not only on Parquet.
- Backup phase is fully skipped if there are no files to delete.

# Related issue

Closes #290

### feature: Poor driver memory handling in GdprDeletion

[Merge request 177](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/177)

Created at: 2020-04-02T10:18:35.200Z

Updated at: 2020-04-15T16:37:49.601Z

Branch: feature/291-gdprdeletion-should-keep-hbase-keys-on-executor-memory-instead-of-driver-one

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

Assignee: [Giuseppe Lillo](https://gitlab.com/giuseppe.lillo)

# New features and improvements

- HBase deletion output of GDPR are eagearly cached on a RDD, loading executors memory instead of being collected and loading driver's one.
- The discovery of files to be rewritten now really uses broadcast join.

# Breaking changes

None.

# Migration

None.

# Bug fixes

- Potential OOM when deleting too many keys from HBase.
- Potential OOM when deleting big files (i.e. always). This was because spark cannot perform a broadcast outer join on the side that is being broadcasted, therefore it broadcasts the other "side" of the join, resulting in OOM due to the *big* dataframes cached on the driver side to be broadcasted.

# Related issue

Closes #291

### feature: Censor CI step is broken

[Merge request 180](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/180)

Created at: 2020-04-07T11:02:05.199Z

Updated at: 2020-04-15T16:37:45.154Z

Branch: feature/292-censor-ci-step-is-broken

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

# New features and improvements

censor step in MR CI builds now works correctly!

# Breaking changes

None.

# Migration

None.

# Bug fixes

Fix MR setup bug that prevented any MR CI Build to pass because the env variable CI_MERGE_REQUEST_TARGET_BRANCH_SHA was always empty.

# Related issue

Closes #292

### Resolve "Add HBase Scan configuration inside GDPR RawModel deletion"

[Merge request 181](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/181)

Created at: 2020-04-08T10:13:32.182Z

Updated at: 2020-04-15T16:37:32.252Z

Branch: feature/287-add-hbase-scan-configuration-inside-gdpr-rawmodel-deletion

Author: [Andrea Consolino](https://gitlab.com/andredc1)

Assignee: [Giuseppe Lillo](https://gitlab.com/giuseppe.lillo)

# New features and improvements

- Add HBase Scan configuration inside GDPR RawModel deletion

# Breaking changes

- Remove `dateIsFirst` use case in GDPR deletion

# Migration

None.

# Bug fixes

None.

# Related issue

Closes #287

### feature: Update Readme - Setting up the development environment

[Merge request 183](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/183)

Created at: 2020-04-09T11:22:59.251Z

Updated at: 2020-04-15T16:37:26.903Z

Branch: feature/294-update-readme-setting-up-the-development-environment

Author: [Fabrizio Spataro](https://gitlab.com/Fabryprog)

# New features and improvements

Add new commands to:
 - build agilelab CDH docker image
 - start wasp

Additionally I refactored the links section at the end of page

# Related issue

Closes #294

### Resolve "Set Partition Pruning Condition as IN instead of between timestamps"

[Merge request 184](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/184)

Created at: 2020-04-10T08:40:48.149Z

Updated at: 2020-04-15T16:37:40.990Z

Branch: feature/295-set-partition-pruning-condition-as-in-instead-of-between-timestamps

Author: [Giuseppe Lillo](https://gitlab.com/giuseppe.lillo)

# New features and improvements

Partition pruning strategies are pushed deeper in the plan.

# Breaking changes

`TimeBasedBetweenPartitionPruningStrategy` now has a granularity argument that specifies the granularity of the date field that is used for partitioning (i.e. Hours, days, seconds ...)

# Migration

Add the new argument of `TimeBasedBetweenPartitionPruningStrategy` if used.

# Bug fixes

None.

# Related issue

Closes #295

### feature: Partition filters are not applied in GDPRDeletionJob

[Merge request 185](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/185)

Created at: 2020-04-13T09:18:52.377Z

Updated at: 2020-04-15T16:37:36.875Z

Branch: feature/296-partition-filters-are-not-applied-in-gdprdeletionjob

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

# New features and improvements

Partition Pruning Strategies of GDPRJob now correctly prunes partitions.

# Breaking changes

None.

# Migration

None.

# Bug fixes

Partition Pruning Strategies of GDPRJob now correctly prunes partitions.

# Related issue

Closes #296

### feature: README snippets have comments and are not highlighted

[Merge request 187](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/187)

Created at: 2020-04-15T10:15:55.288Z

Updated at: 2020-04-15T16:37:22.680Z

Branch: feature/301-readme-snippets-have-comments-and-is-not-highlighted

Author: [Antonio Murgia](https://gitlab.com/antonio.murgia)

Assignee: [Andrea Fonti](https://gitlab.com/andrea.fonti)

# New features and improvements

Remove the comments and turn on syntax highlighting in README.md.

# Breaking changes

None.

# Migration

None.

# Bug fixes

None.

# Related issue

Closes #301

### feature: Promote docker-new whitelabel to official whitelabel

[Merge request 188](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/188)

Created at: 2020-04-15T13:23:33.904Z

Updated at: 2020-04-15T16:37:18.629Z

Branch: feature/302-promote-docker-new-whitelabel-to-official-whitelabel

Author: [Andrea Fonti](https://gitlab.com/andrea.fonti)

# New features and improvements

- Promoted `docker-new` docker local deployment to be the "official" one
- Updated readme with new gitter room and instructions for new whitelabel

# Breaking changes

None.

# Migration

None.

# Bug fixes

None.

# Related issue

Closes #302

## WASP 2.25.0

### feature: SQL Event Engine

[Merge request 118](https://gitlab.com/AgileFactory/Agile.Wasp2/-/merge_requests/118)

Created at: 2019-02-15T11:07:23.071Z

Updated at: 2020-04-16T17:13:43.804Z

Branch: feature/195-sql-event-engine-event-structure

Author: [Alessandro D'Armiento](https://gitlab.com/alessandro.darmiento)

# EVENT ENGINE #

The Event Engine main functionality is to analyze data in transit,
applying user-defined rules and generating events that are notified to
the user itself.

![EventEngineOverview](documentation/images/EventEngineOverview.png)

The Event Engine, in the current first release, is decoupled from the
mail dataflow by mean of one or more Kafka queues:

-   Messages in the application data model are dequeued by an Event
    Producer from a user defined Kafka queue

    -   Many Event Producer can consume from the same Kafka queue.

-   Generated Event objects, are enqueued in a (different) user defined
    Kafka queue.

    -   Many events flow can converge in the same Kafka queue.

-   Event objects, which are compliant to a specific data model, can be
    consumed by an Event Consumer, which send them as e-mails to the
    user

## Event: ##

An Event is an object that contains information relating to a
criticality or an unexpected situation, generated from a series of rules
defined by the user. Event objects are characterized by:

-   Event Id: Unique identifier of the Event, generated via an UUID

-   Timestamp: UTC instant of the generation of the Event, with
    millisecond resolution

-   Event Type: String representing the type of Event

-   Severity: String representing the Degree of severity of the Event

-   Source Id: Optional identifier of the original data that generated
    the Event

-   Source: Identifier of the streaming data source from which the Event
    was generated

-   Payload: json transcription of the entire data that generated the
    Event

-   Event Rule Name: Name of the rule responsible for the generated
    event object

Among these fields, eventId, timestamp, payload, source and
eventRuleName are managed internally, while eventType, severity and
sourceId can be customized by the user.

## Mail: ##

A Mail object is a pre-packaged item ready to be sent as a notification
of a occurrent Event. Mail objects are characterized by:

Mail To: Main recipient of the e-mail

Mail CC: Optional carbon copy recipient of the e-mail

Mail BCC: Optional hidden carbon copy recipient of the -e-mail

Mail Subject: Subject of the email

Mail Content: Content of the mail

All fields can be customized by the user.

The content of the email, in text/html format, is generated by the
Apache Velocity templating engine, using user provided template files,
compiled at runtime with the information available from Event object.

As the current implementation, each Event object may generate up to one
Mail.

## Configuration: ##

The Event Engine is provided with two Pipegraphs, the user can access
and configure them editing the ***it.agilelab.wasp.eventengine***
section of the configuration file.

Every configuration section mentioned from now on has to be considered
as child of the ***it.agilelab.wasp.eventengine*** block.

## Event Pipegraph: ##

The Event Pipegraph is the Pipegraph which is in charge of generate
Event objects. Event Pipegraph configuration is accessible inside the
block ***eventPipegraph***, which exposes the following keys:

-   ***isSystem***: flag which defines whether the Event Pipegraph
    should be included in the system pipegraphs (and therefore be
    automatically executed at startup when ***systempipegraphs.start***
    is enabled)

-   ***eventStrategy***: array of configuration objects in charge of
    creating each one an Event Producer.

Each element of the ***eventStrategy*** array is in turn composed by the
following keys:

-   ***name***: Name of the input streaming data source

-   ***reader***: configuration block that defines how to access input
    data from a Kafka queue

-   ***writer***: configuration block that defines where to write the
    output Event objects in a Kafka queue

-   ***trigger***: list of rules for generating Event objects

The configuration block is defined as follows:

```hocon
eventPipegraph: {

    IsSystem: true

    eventStrategy: [

      {

        name: “*the name of the event strategy*"

        reader: {...}

        writer: {...}

        trigger: {...}

     },

      {

        name: “*the name of the event strategy*"

        reader: {...}

        writer: {...}

        trigger: {...}

     },

     {...},

     {...}

  ]

}
```

In turn, child objects are defined as follows:

```hocon
reader: {

          modelName: "*The identifier in MongoDB of the input model*"

          datastoreModelName: "*name of the kafka topic*"

          modelType: "Kafka"

          options: [

            {

              key: "rate-limit"

              value: "200"

            }

        ]

}


writer: {

          modelName: "*The identifier in MongoDB of the output model*"

          datastoreModelName: "*name of the kafka topic*"

          modelType: "Kafka"

          options: [ (*MapLike sequence of Key→Value option)*

            {

              key: "replica"

              value: "3"

            },

            {

              key: "partitions"

              value: "3"

            },

       ]

   }



trigger: {

          eventRules: [

            {...},

            {...}

          ]

        }
```

Where each element of the eventRules array is defined as follows
```hocon
{

     name: "*name of the specific rule*"

     streamingSource = "*name of the streaming data source*",

     statement = "*SQL WHERE clause to infer whether or not an Event is
occurred*",

     typeExpression = "*SQL WHERE clause to decorate the Event with the
eventType field*",

     severityExpression = "*SQL WHERE clause to decorate the Event with
the severity field*",

     sourceIdExpression = "*SQL WHERE clause to decorate the Event with
the sourceId field*",

}
```

For each user-defined ***eventStrategy*** object, a minimum of one event
rule has to be defined in the ***eventRules*** array. There is however
no maximum limit imposed by the application on the number of rules for
each ***eventRules*** array. Hence, the user can add, remove or freely
modify event rules and restart Wasp to put them into operation.

 ![EventPipegraphExample](documentation/images/EventPipegraphExample.png)

In the example, the Pipegraph Event has been configured to generate 4
Event Producers (therefore equivalent to four elements in the
***eventStrategy*** array). Each ETL is defined by entering an entry
point (reader) and an exit point (writer). Multiple ETLs can read from
the same input queue and / or write to the same output queue.



## Mailing Pipegraph: ##

The Mailing Pipegraph is the Pipegraph which is in charge of consume
Event objects. Mailing Pipegraph configuration is accessible inside the
block ***mailingPipegraph***, which exposes the following keys:

-   ***isSystem***: flag which defines whether the Event Pipegraph
    should be included in the system pipegraphs (and therefore be
    automatically executed at startup when ***systempipegraphs.start***
    is enabled)

-   ***writer***: configuration block that defines how to connect to an
    SMTP server and send e-mails

-   ***mailingStrategy***: array of configuration objects in charge of
    creating each one an Event Producer.

Each element of the ***mailingStrategy*** array is in turn composed by
the following keys:

-   ***name***: Name of the input streaming Event source

-   ***reader***: configuration block that defines how to access Event
    data from a Kafka queue

-   ***trigger***: list of rules for consuming Event objects

The configuration block is defined as follows:

```hocon
mailingPipegraph: {

    isSystem: false

    writer: {...}

    mailingStrategy: [

      {

name: “*the name of the mailing strategy*”

              reader: {...}

              trigger: {...}

      },

      {...}

   ]

  }
```

The ***writer*** block is defined as follows:

```hocon
writer: {

      modelName: "*Name of the mail writer*"

      datastoreModelName: "*Name of the topic from which fetch Event*"

      modelType: "Mail"

      options: [

        {

          key: "mail.smtp.host"

          value: "*SMTP server url or ip address*"

        }

        {

          key: "mail.smtp.port"

          value: "*SMTP port*"

        },

        {

          key: "mail-from"

          value: "*SMTS server’s accredited mail to be used as sender
address*"

        },

        {

          key: "mail.smtp.auth"

          value: "*Flag indicating if an authentication step on the SMPT
server is required*"

        },

        {

          key: "mail.smtp.ssl.enable"

          value: "*Flag indicating if the conncection over SSL is
enabled*"

        },

        {

          key: "username"

          value: "*Username to connect to the SMTP server*"

        },

        {

          key: "password"

          value: "Password *to connect to the SMTP server*"

        },

      ]

    }
```

The user is left with the task of compiling the ***options*** array,
entering all the information needed to connect to the chosen SMTP
server.

Each element of the ***mailingStrategy*** array has the following
children:

```hocon
reader: {

          modelName: "*Name of the event input model*"

          datastoreModelName: "*Name of the Kafka topic to fetch Event
objects from*"

          modelType: "Kafka"

          options: [

            {

              key: "rate-limit"

              value: "200"

            }

          ]

        }


trigger: {

          mailRules: [

                       {... },

          {...}

          ]

        }
```


In which each element of the ***mailRules*** array is defined as
follows:

```hocon
{

              name: "Name of the mailing rules"

              statement: "*SQL WHERE clause to decide whether or not a
Mail should be generated from the Event*"

              subjectExpression: "*SQL WHERE clause to decorate the Mail
with the subject*"

              templatePath: "*Path to a Velocity template file available
on the Spark Driver*"

              recipient: {

                mailTo: “*Comma separated main recipients of the Mail*"

                mailCc: “*Comma separated optional CC recipient of the
Mail*"

                mailBcc: "*Comma separated optional BCC recipient of the
Mail*"

              }

}
```
For each
user-defined ***mailingStrategy*** object, a minimum of one mailing rule
has to be defined in the ***mailingRules*** array. There is however no
maximum limit imposed by the application on the number of rules for each
***mailingRules*** array. Hence, the user can add, remove or freely
modify mailing rules and restart Wasp to put them into operation.

![MailingPipegraphExample](documentation/images/MailingPipegraphExample.png)

In the example, the Mailing Pipegraph has been configured to generate 4
Event Consumer (therefore equivalent to four elements in the
***mailingStrategy*** array). Each ETL is defined by entering an entry
point (***reader***), while the same exit point (***writer***) is used
for all ETLs. Multiple ETLs can read from the same input queue.

The example pipeline, complete with Event Pipegraph and Mailing
Pipegraph, is as follows:

![EventEngine](documentation/images/EventEngine.png)

## Velocity Template: ##

Apache Velocity (`https://velocity.apache.org/`) is a Template Engine for
generating web pages, documents, SQL queries, etc.

In the context of the Event Engine, Velocity is used for mail
composition. The Mailing Pipegraph will generate the contents of the
mail starting from the template provided in the Velocity template file
(.vm) and the contents of the Event object.

The contents of the fields of the Event object can be referenced in the
Velocity template via the notation `${field}`

Below the fields currently supported and how they have to be referenced:

```velocity
eventId = ${eventId}

eventType = ${eventType}

severity = ${severity}

payload = ${payload}

timestamp = ${timestamp}

source = ${source}

sourceId = ${sourceId}

ruleName = ${ruleName}
```

Closes #195
Closes #197