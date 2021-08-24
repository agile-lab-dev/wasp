#### Services
![components](diagrams/components.png)

##### Kafka
Kafka is the central element of this architecture blue print.
Each topic must have an associated Avro schema. This enforces type consistency and is the first step towards reliable real time data quality, something we will work on in the next future.
Avro has been chosen because more typed and descriptive than JSON and because of its compatibility with Spark and the Hadoop world in general.
Kafka decouples the ingestion layer from the analysis one. This allows updating algorithms and models without impacting the ingestion layer, and vice versa.

##### Spark
Spark is the data engine powering WASP, and is used in two components: Streaming ETL and Batch ETL. It can also be used to provide a JDBC interface using Thrift server.
WASP supports running Spark in three different ways:
- embedded, using Spark's local mode, which is recommended for development only
- on YARN, used when running with an existing Hadoop cluster
- with Spark's standalone clustering (master + workers)
- on K8s leveraging Spark capability to run on kubernets

##### Akka
Akka is our middleware: each component of WASP is an actor and relies on a clustered Actor System. In this way each component can be a separate process, and even run on different machines, and we can handle fault tolerance in a trasparent way to the whole application.
This is a general overview of the Actor System.
![ActorSystem](diagrams/actor_system.png)

##### Persistent OLTP Database
Wasp features a central repository for all configurations, ML models, and entities. It currently features two different database implementations:

1. **MongoDB**: It is fault tolerant and it simplifies the deployment in a distributed environment because each node just needs the MongoDB address to be ready to go.
2. **PostgreSQL**

##### Pluggable Datastore

WASP system is integrated with Elasticsearch, Solr, Kafka, HBase, Mongo, Jdbc Datasources and HDFS. All data stored inside the datastore is indexed and searchable via the specific query language of the datastore.