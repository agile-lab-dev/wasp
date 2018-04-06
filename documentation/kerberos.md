## Cosa fare per configurare kerberos 

- Cambiare il file docker-environment.conf in modo che spark si connetti ad un cluster yarn kerberizzato seguendo la guida [yarn.md](yarn.md)
- Configurare le configurazioni di sicurezza per kafka:
            
        others = [
          # mandatory
          { "security.protocol" = "SASL_PLAINTEXT" }
          { "sasl.kerberos.service.name" = "kafka" }
          { "sasl.jaas.config" = "com.sun.security.auth.module.Krb5LoginModule required storeKey=true useKeyTab=true useTicketCache=false keyTab=\"./wasp2.keytab\" serviceName=\"kafka\" principal=\"wasp2@REALM\";" }
          { "sasl.mechanism" = "GSSAPI" }
          { "kafka.security.protocol" = "SASL_PLAINTEXT" }
          { "kafka.sasl.kerberos.service.name" = "kafka" }
          { "kafka.sasl.jaas.config" = "com.sun.security.auth.module.Krb5LoginModule required storeKey=true useKeyTab=true useTicketCache=false keyTab=\"./wasp2.keytab\" serviceName=\"kafka\" principal=\"wasp2@REALM\";" }
          { "kafka.sasl.mechanism" = "GSSAPI" }
        
          # optional
          { "sasl.kerberos.kinit.cmd" = "/usr/bin/kinit" }
          { "sasl.kerberos.min.time.before.relogin" = "60000" }
          { "sasl.kerberos.ticket.renew.jitter" = "0.05" }
          { "sasl.kerberos.ticket.renew.window.factor" = "0.8" }
          { "kafka.sasl.kerberos.kinit.cmd" = "/usr/bin/kinit" }
          { "kafka.sasl.kerberos.min.time.before.relogin" = "60000" }
          { "kafka.sasl.kerberos.ticket.renew.jitter" = "0.05" }
          { "kafka.sasl.kerberos.ticket.renew.window.factor" = "0.8" }
        ]
            
- Creare un keytab da inserire sotto la cartella whitelabel/docker (seguire le istruzioni del file whitelabel/docker/keytab-build.sh per creare il keytab )
- Andare nel file whitelabel/docker/security-env.sh e modificare le variabili di ambiente con i valori corretti:
    - KEYTAB_FILE_NAME è il nome del file del keytab creato ex. wasp2.keytab
    - PRINCIPAL_NAME è il principal compreso di realm usato per effettuare il login  ex. nome.cognome@CLUSTER01.ATSCOM.IT
    - ETC_HOSTS è una variabile usata per fornire configurazioni aggiuntive a docker, in questo caso si dovrebbero aggiungere tutti gli host con relativi ip
      a cui spark si dovrebbe collegare questo succede quando il dns non ha il reverse o non è configurato per fornire informazioni riguardanti kerberos
      ex. "--add-host=server08.cluster01.atscom.it:192.168.69.223"
    - Controllare che le opzioni presenti nel file krb5.conf che viene generato siano corrette 
- Lanciare whitelabel/docker/start-whitelabel-wasp.sh con l'opzione -s oppure -security

## Remembers 

- Ricordarsi di configurare la checkpoint-dir di spark streaming e batch dentro il file di configurazione di wasp, questo percorso deve essere accessibile all'utente
- Nelle configurazioni di spark-streaming e spark-batch mettere queste configurazioni aggiuntive

        others = [
          { "spark.yarn.dist.files" = "file:///root/configurations/wasp2.keytab,file:///root/configurations/sasl.jaas.config" }
          { "spark.executor.extraJavaOptions" = "-Djava.security.auth.login.config=./sasl.jaas.config" }
        ]
    IMPORTANTE mettere file:// davanti al percorso dei file altrimenti non vengono trasferti
- Non cambiare il nome al keytab altrimenti modificate l'MD5 e l'autenticazione non fuziona più
     
### Problema 
Nel caso di questa eccezione significa che il reverse dns degli indirizzi del cluster sulla macchina/docker su cui è eseguito wasp non funziona

    java.io.IOException: Failed on local exception: java.io.IOException: java.lang.IllegalArgumentException: Server has invalid Kerberos principal: yarn/principal@REALM; Host Details : local host is: ""; destination host is: "SERVER":8032;
      at org.apache.hadoop.net.NetUtils.wrapException(NetUtils.java:772)
      at org.apache.hadoop.ipc.Client.call(Client.java:1474)
      at org.apache.hadoop.ipc.Client.call(Client.java:1401)
      at org.apache.hadoop.ipc.ProtobufRpcEngine$Invoker.invoke(ProtobufRpcEngine.java:232)
      at com.sun.proxy.$Proxy16.getNewApplication(Unknown Source)
      at org.apache.hadoop.yarn.api.impl.pb.client.ApplicationClientProtocolPBClientImpl.getNewApplication(ApplicationClientProtocolPBClientImpl.java:217)
      at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
      at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
      at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
      at java.lang.reflect.Method.invoke(Method.java:498)
      at org.apache.hadoop.io.retry.RetryInvocationHandler.invokeMethod(RetryInvocationHandler.java:187)
      at org.apache.hadoop.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:102)
      at com.sun.proxy.$Proxy17.getNewApplication(Unknown Source)
      at org.apache.hadoop.yarn.client.api.impl.YarnClientImpl.getNewApplication(YarnClientImpl.java:206)
      at org.apache.hadoop.yarn.client.api.impl.YarnClientImpl.createApplication(YarnClientImpl.java:214)
      at org.apache.spark.deploy.yarn.Client.submitApplication(Client.scala:159)
      at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.start(YarnClientSchedulerBackend.scala:56)
      at org.apache.spark.scheduler.TaskSchedulerImpl.start(TaskSchedulerImpl.scala:173)
      at org.apache.spark.SparkContext.<init>(SparkContext.scala:509)
      at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2516)
      at org.apache.spark.sql.SparkSession$Builder$$anonfun$6.apply(SparkSession.scala:918)
      at org.apache.spark.sql.SparkSession$Builder$$anonfun$6.apply(SparkSession.scala:910)
      at scala.Option.getOrElse(Option.scala:121)
      at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:910)
      at org.apache.spark.repl.Main$.createSparkSession(Main.scala:101)
      ... 47 elided
    Caused by: java.io.IOException: java.lang.IllegalArgumentException: Server has invalid Kerberos principal: yarn/principal@REALM
      at org.apache.hadoop.ipc.Client$Connection$1.run(Client.java:682)
      at java.security.AccessController.doPrivileged(Native Method)
      at javax.security.auth.Subject.doAs(Subject.java:422)
      at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1692)
      at org.apache.hadoop.ipc.Client$Connection.handleSaslConnectionFailure(Client.java:645)
      at org.apache.hadoop.ipc.Client$Connection.setupIOstreams(Client.java:732)
      at org.apache.hadoop.ipc.Client$Connection.access$2800(Client.java:370)
      at org.apache.hadoop.ipc.Client.getConnection(Client.java:1523)
      at org.apache.hadoop.ipc.Client.call(Client.java:1440)
      ... 70 more
    Caused by: java.lang.IllegalArgumentException: Server has invalid Kerberos principal: yarn/principal@REALM
      at org.apache.hadoop.security.SaslRpcClient.getServerPrincipal(SaslRpcClient.java:334)
      at org.apache.hadoop.security.SaslRpcClient.createSaslClient(SaslRpcClient.java:231)
      at org.apache.hadoop.security.SaslRpcClient.selectSaslClient(SaslRpcClient.java:159)
      at org.apache.hadoop.security.SaslRpcClient.saslConnect(SaslRpcClient.java:396)
      at org.apache.hadoop.ipc.Client$Connection.setupSaslConnection(Client.java:555)
      at org.apache.hadoop.ipc.Client$Connection.access$1800(Client.java:370)
      at org.apache.hadoop.ipc.Client$Connection$2.run(Client.java:724)
      at org.apache.hadoop.ipc.Client$Connection$2.run(Client.java:720)
      at java.security.AccessController.doPrivileged(Native Method)
      at javax.security.auth.Subject.doAs(Subject.java:422)
      at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1692)
      at org.apache.hadoop.ipc.Client$Connection.setupIOstreams(Client.java:719)
      ... 73 more

### Soluzione
- Far funzionare il reverse dns 
- Mettere gli hosts che mancano dentro /etc/hosts
- https://docs-snaplogic.atlassian.net/wiki/spaces/SD/pages/2015960/How+to+Configure+a+Groundplex+for+CDH+with+Kerberos+Authentication