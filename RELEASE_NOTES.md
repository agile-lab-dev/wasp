### WASP 2.0.5 ###

- Miglioramento complessivo error/exception-handling dai seguenti punti di vista:
	- log in console dei vari componenti
	- propagazione e gestione errori - ora riportati fino alla REST-response
	
	N.B. il timeout passato alla WaspSystem.??() (se non esplicitato viene usato general-timeout-millis di *.conf) è ora inteso come "tempo complessivo per gestire la REST" e non più come "tempo per svolgere la specifica operazione interna (es. avvio pipegraph)": a partire da general-timeout, lo slot di tempo assegnato ai livelli inferiori di annidamento è via via ridotto di 5sec

- Gestione down(post-unreachable) / reJoin(post-reachable) dei membri XyzMasterGuardian del cluster Akka - gestito tramite ClusterListenerActor (e actor-downing-timeout-millis di *.conf)
	
	N.B. almeno un seed-node deve rimane in vita per poter fare reJoin!!!

- Gestione launcher tramite CommanLine invece che lista argomenti


### WASP 2.1.0 ###

Fix
- A seguito di failure di StartPipegraph, SparkConsumersMasterGuardian e RtConsumersMasterGuardian non rimangono più in stato "starting" ma ritornano in "unitialized", evitando quindi lo stash() di RestartConsumers durante StartPipegraph successivi

- Corretto uso di log4j direttamente da WASP framework

Update
- Miglioramento complessivo error/exception-handling durante StopProducer e StopPipegraph

- Log di stackTrace al posto del solo message da parte dell'attore che gestisce l'eccezione (continua ad esser propagato il solo message)

- Allineata cartella docker (yml e sh) per futuro uso WhiteLabel

- Solr unique key: IndexModel accetta parametro opzionale 'idField' per indicare quale campo usare come id al posto di autogenerare UUID random

- Elastic Spark upgrade to 6.1 for Structured Streaming

	yml di riferimento rimane 'docker/elastickibana-docker-compose.yml'

- Gestione parametri commandLine e relativo allineamento 'docker/start-wasp.sh'
	
	Parametri disponibili: -h (help), -v (versione), -d (MongoDB dropping)
		
		-h, -v	ricevuto da tutti
		-d	ricevuto solo da master

- Aggiornamento di 'reference.conf': WASP usa ora i default presi da 'reference.conf'; 'docker/docker-environment.conf' funge per ora come "template-whitelabel" dove sono presenti le KEY da sovrascrivere obbligatorie e invece commentate tutte le altre KEY possibili
	
	N.B. per mantenere scalabile la soluzione, i VALUE di default presenti in 'reference.conf' non sono anche riportati in 'docker/docker-environment.conf'


### WASP 2.1.1 ###
Fix
- GitLab CI to compile and publish on internal (nexus) and public (criticalcase)


### WASP 2.1.2 ###
Fix
- GitLab CI rimossa da master
- Scommentate le KEY 'driver-hostname' della "template-whitelabel" 'docker/docker-environment.conf' di 'spark-streaming' 'spark-batch'

Update
- HBASE Writer - gestione celle create dinamicamente in stile Cassandra


### WASP 2.1.3 ###
Update
- MongoDB fullwriteConsistency


### WASP 2.2.0 ###
Fix
- Corretta la KEY 'driver-hostname' della "template-whitelabel" 'docker/docker-environment.conf' di 'spark-batch'
- MongoDB, Elastic, Solr considerano ora il timeout di configuration espresso in millis

Update
- WaspRELEASE_NOTES + WhiteLabelREADME

- Trait 'Strategy' estende 'Serializable'

- Revisione connectionTimeout verso MongoDB, Solr, Elastic

- Impostazione WhiteLabel (per usarla: 'whitelabel/docker/start-whitelabel-wasp.sh')

- Riportata scrittura di tutte le configurazioni MongoDB fatta da tutti (in WaspSystem)

- Revisione della gestione "dropDB" tramite commandlineOption '-d' di 'start_wasp.sh': Master fa solo drop ed esce (senza reinizializzare)

- Modulo consolePlugin separato da modulo consumers-spark

- Migrazione totale cross-reference da byId a byName delle collection MongoDB


### WASP 2.3.0 ###
Update
- Consistenza/atomicità su waspDB.insertIfNotExist: permette che non avvengano scritture contemporanee/duplicate

- Supporto Solr per nested document

- Whitelabel manual/auto-test per Console, Solr, HDFS, ElasticSearch

- LoggerPipegraph su Solr tramite StructuredStreaming 

- Aggiunto 'banana' per Solr service in 'whitelabel/docker/solrcloud-docker-compose.yml'

- Batch separato da streaming (container apposito) ma in stesso modulo consumers-spark