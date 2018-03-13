## Configurazioni

- Ricordarsi di configurare queste variabili 
  
        yarn-jar = "hdfs://server08.cluster01.atscom.it:8020/tmp/spark-jars-yarn-2.2.1/*"
        additionalJarsPath = "file:///root/wasp/lib/"
      
  
- Bisogna mettere su HDFS le librerie di spark che poi verranno usate per lanciare gli executors 
    - Scaricare il tar https://www.apache.org/dyn/closer.lua/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz
    - Decomprimerlo 
    - Copiare il contenuto della folder spark-2.2.1-bin-hadoop2.6/jars e di spark-2.2.1-bin-hadoop2.6/yarn su hdfs, per esempio in hdfs://nameserver/tmp/spark-jars-yarn-2.2.1//tmp/spark-jars-yarn-2.2.1/
    - Assegnare alla configurazione yarn-yar il path dove sono presenti i jar  hdfs://nameserver/tmp/spark-jars-yarn-2.2.1/
   
- Oltre alle configurazioni di ram e cpu per fa funzionare wasp in yarn mode bisogna copiare nella folder external-cluster-configuration/hadoop
  tutte le configurazioni di yarn e hbase (se si utilizza).
  Queste configurazioni sono facilmente recuperabili nel cloudera manager sotto la voce del servizio YARN Download client configuration, la stessa cosa per il servizio HBase.
  
- Dopo aver scaricato queste configurazioni bisogna copiarle nella folder external-cluster-configuration/hadoop
  e controllare che la configurazine della topology (net.topology.script.file.name) dentro il file core-site.xml che è da cancellare

- Invece per quanto rigurda le configurazioni docker-environment.conf modificare il seguente esempio

        master = {
          protocol = ""
          host = "yarn" # Dice a spark di abilitare la modalità yarn
          port = 0
        }
        driver-conf {
          submit-deploy-mode = "client" # modalità yarn-client
          driver-cores = 1              
          driver-memory = "1G"
          driver-hostname = "server08.cluster01.atscom.it" # Questo è l'hostname che gli executor contattano per comunicare con il driver
          driver-bind-address =  "0.0.0.0"
          driver-port = "31541"
        }
        block-manager-port = "31542"
        yarn-jar = "hdfs://server08.cluster01.atscom.it:8020/tmp/spark-jars-yarn-2.2.1/*" # La folder dove sono presenti tutti i jars di Spark per gli executors 
        additionalJarsPath = "/root/wasp/lib/" # Percorso di dove spark va a prendere i jar utente 
        executor-cores = "1"
        executor-memory = "1G"
        executor-instances = "1"
        retained-stages-jobs = 100
        retained-tasks = 5000
        retained-executions = 100
        retained-batches = 100
        retained-jobs = 100
        checkpoint-dir = "/user/mattia.bertorello/checkpointstreaming"
        others = [
            # Per impostare la sicurezza bisogna mettere nella folder del executor il keytab dell'utente e il jaas.config per i client zookeeper e kafka 
          { "spark.yarn.dist.files" : "file:///root/configurations/mattia.bertorello.keytab,file:///root/configurations/sasl.jaas.config" }
            # Si imposta il percorso del jaas file in modo che un client che supporta sasl sappia dove andarlo a prendere 
          { "spark.executor.extraJavaOptions" : "-Djava.security.auth.login.config=./sasl.jaas.config" }
            # Dice a spark di abilitare l'autenticazione ai servizi Yarn, HDFS, HBase
          { "spark.authenticate" : "true" }
        
        ]
- Ricordarsi di cambiare le driver-port e block-manager-port in modo che siano diverse tra lo streaming e il batch 

- Lanciare con l'opzione --yarn