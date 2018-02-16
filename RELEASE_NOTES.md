### WASP 2.0.5 ###

- Miglioramento complessivo error/exception-handling dai seguenti punti di vista:
	- log in console dei vari componenti
	- propagazione e gestione errori - ora riportati fino alla REST-response
	
	N.B. il timeout passato alla WaspSystem.??() (se non esplicitato viene usato general-timeout-millis di *.conf) è ora inteso come "tempo complessivo per gestire la REST" e non più come "tempo per svolgere la specifica operazione interna (es. avvio pipegraph)": a partire da general-timeout, lo slot di tempo assegnato ai livelli inferiori di annidamento è via via ridotto di 5sec

- Gestione down(post-unreachable) / reJoin(post-reachable) dei membri XyzMasterGuardian del cluster Akka - gestito tramite ClusterListenerActor (e actor-downing-timeout-millis di *.conf)
	
	N.B. almeno un seed-node deve rimane in vita per poter fare reJoin!!!

- Gestione launcher tramite CommanLine invece che lista argomenti
	
	N.B. quando ISC/VA deciderà di passare a questa nuova versione di WASP dovrà modificare "override def launch" dei XyzNodeLauncher


### WASP 2.1.0 ###

Fix
- A seguito di failure di StartPipegraph, SparkConsumersMasterGuardian e RtConsumersMasterGuardian non rimangono più in stato "starting" ma ritornano in "unitialized", evitando quindi lo stash() di RestartConsumers durante StartPipegraph successivi

- Corretto uso di log4j direttamente da WASP framework

Update

1- Miglioramento complessivo error/exception-handling durante StopProducer e StopPipegraph

2- Log di stackTrace al posto del solo message da parte dell'attore che gestisce l'eccezione (continua ad esser propagato il solo message)

3- Allineata cartella docker (yml e sh) per futuro uso WhiteLabel

4- Solr unique key: IndexModel accetta parametro opzionale 'idField' per indicare quale campo usare come id al posto di autogenerare UUID random

5- Elastic Spark upgrade to 6.1 for Structured Streaming

	yml di riferimento rimane 'docker/elastickibana-docker-compose.yml'

6- Gestione parametri commandLine e relativo allineamento 'docker/start-wasp.sh'
	
	Parametri disponibili: -h (help), -v (versione), -d (MongoDB dropping)
		
		-h, -v	ricevuto da tutti
		-d	ricevuto solo da master

7- Aggiornamento di 'reference.conf': WASP usa ora i default presi da 'reference.conf'; 'docker/docker-environment.conf' funge per ora come "template-whitelabel" dove sono presenti le KEY da sovrascrivere obbligatorie e invece commentate tutte le altre KEY possibili
	
	N.B. per mantenere scalabile la soluzione, i VALUE di default presenti in 'reference.conf' non sono anche riportati in 'docker/docker-environment.conf'


### WASP 2.1.1 ###
Fix
- GitLab CI to compile and publish on internal (nexus) and public (criticalcase)


### WASP 2.1.2 ###
Fix
- GitLab CI rimossa da master
- Scommentate le KEY 'driver-hostname' della "template-whitelabel" 'docker/docker-environment.conf' di 'spark-streaming' 'spark-batch', KEY obbligatorie da sovrascrivere nei verticali ISC/VA

Update
- HBASE Writer - gestione celle create dinamicamente in stile Cassandra