# WASP RESTful APIs

Wasp restful apis are documented via OpenApi v3, the idl file is located here: `documentation/wasp-openapi.yaml`

Base server URL: `http://localhost:2891`

### Pipegraphs

Available commands:

- GET `http://localhost:2891/pipegraphs` -> lists available pipegraphs
- GET `http://localhost:2891/pipegraphs/{pipegraph_name}` -> shows info about a specific pipegraph
- GET `http://localhost:2891/pipegraphs/{pipegraph_name}/instances` -> get instances of the specified pipegraph
- GET `http://localhost:2891/pipegraphs/{pipegraph_name}/instances/{id}` -> et instance status of the specified pipegraph instance
- POST `http://localhost:2891/pipegraphs/{pipegraph_name}/start` -> start a pipegraph
- POST `http://localhost:2891/pipegraphs/{pipegraph_name}/stop` -> stop a pipegraph
- DELETE `http://localhost:2891/pipegraphs/{pipegraph_name}` -> delete a pipegraph

### Producers

Available commands:

- GET `http://localhost:2891/producers` -> lists available producers
- GET `http://localhost:2891/producers/{producer_name}` -> shows info about a specific producer
- PUT `http://localhost:2891/producers/{producer_name}` -> update a producer
- POST `http://localhost:2891/producers/{producer_name}/start` -> start a producer
- POST `http://localhost:2891/producers/{producer_name}/stop` -> stop a producer

### Batch Jobs

Available commands:

- GET `http://localhost:2891/batchjobs` -> lists available producers
- PUT `http://localhost:2891/batchjobs` -> insert a new batch job
- POST `http://localhost:2891/batchjobs` -> update an existing batch job
- GET `http://localhost:2891/batchjobs/{id}` -> get info about specific batch job
- DELETE `http://localhost:2891/batchjobs/{id}` -> delete a batch job
- POST `http://localhost:2891/batchjobs/{id}/start` -> start an instance of a batch job(JSON Config optional)
- GET `http://localhost:2891/batchjobs/{id}/instances` -> get instances of the specified batch job
- GET `http://localhost:2891/batchjobs/{id}/instances/{id}` -> get instance status of the specified batch job instance

### Topics

Available commands:

- GET `http://localhost:2891/topics` -> get all the topics in the system
- GET `http://localhost:2891/topics/{id}` -> get the specific topic

### Configs

Available commands:

- GET `http://localhost:2891/configs/kafka` -> get the kafka config
- GET `http://localhost:2891/configs/sparkbatch` -> get the spark batch config
- GET `http://localhost:2891/configs/solr` -> get the solr config
- GET `http://localhost:2891/configs/es` -> get the elasticSearch config
- GET `http://localhost:2891/configs/sparkstreaming` -> get the spark streaming config

### ML Models

- GET `http://localhost:2891/mlmodels` -> get all the ML models in the system
- PUT `http://localhost:2891/mlmodels` -> update a ML Model
- GET `http://localhost:2891/mlmodels/{id}` -> get a specific ML Model
- DELETE `http://localhost:2891/mlmodels/{id}` -> delete a specific ML Model

### Indexes

- GET `http://localhost:2891/indexes` -> get all the indexes in the system
- GET `http://localhost:2891/indexes/{id}` -> get the specific index
