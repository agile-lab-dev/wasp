FROM apache/nifi-stateless:1.11.4 AS nifi

FROM ubuntu:16.04

COPY --from=nifi /opt/nifi/nifi-current/lib/*.jar /nifi-libs/system/
COPY --from=nifi /opt/nifi/nifi-current/lib/bootstrap/*.jar /nifi-libs/bootstrap/
COPY --from=nifi /opt/nifi/nifi-current/work/stateless-nars /nifi-libs/extensions

RUN set -ax \
    && apt-get update \
    && apt-get install -y apt-transport-https \
    && apt-get install -y ca-certificates wget openjdk-8-jdk software-properties-common python-software-properties sudo \
    && apt-get install -y gettext-base multitail supervisor \
    && rm -rf /var/lib/apt/lists/*

COPY cloudera.list /etc/apt/sources.list.d/cloudera.list

RUN set -ax \
    && wget https://archive.cloudera.com/cdh6/6.3.2/ubuntu1604/apt/archive.key \
    && apt-key add archive.key \
    && wget https://archive.cloudera.com/kafka/ubuntu/xenial/amd64/kafka/pool/contrib/k/kafka/kafka_0.11.0+kafka3.0.0-1.3.0.0.p0.50~xenial-kafka3.0.0_all.deb \
    && wget https://archive.cloudera.com/kafka/ubuntu/xenial/amd64/kafka/pool/contrib/k/kafka/kafka-server_0.11.0+kafka3.0.0-1.3.0.0.p0.50~xenial-kafka3.0.0_all.deb\
    && apt-get update \
    && apt-get install -y --allow-unauthenticated hadoop-yarn-resourcemanager hadoop-hdfs-namenode hadoop-yarn-nodemanager hadoop-hdfs-datanode hadoop-mapreduce hadoop-mapreduce-historyserver hadoop-yarn-proxyserver hbase spark-core spark-history-server hbase-master hbase-regionserver zookeeper-server=3.4.5+cdh6.3.2-1605554 zookeeper=3.4.5+cdh6.3.2-1605554 solr-server \
    && dpkg -i kafka*.deb \
    && rm -rf /var/lib/apt/lists/*


RUN set -axe && \
     mkdir -p /nifi-libs/stateless && \
     curl -fLo cs https://git.io/coursier-cli-linux && \
     chmod +x cs && \
     bash -c "./cs fetch org.apache.nifi:nifi-stateless:1.11.4 org.apache.nifi:nifi-nar-utils:1.11.4 | xargs -I{} cp {} /nifi-libs/stateless" && \
     ls /nifi-libs/stateless


ENV TEMPLATE_DIR /templates


COPY entrypoint.sh /usr/bin/entrypoint.sh
COPY resolve-templates.sh /usr/bin/resolve-templates.sh
COPY hdfs/core-site.xml ${TEMPLATE_DIR}/core-site.xml
COPY hbase/hbase-site.xml ${TEMPLATE_DIR}/hbase-site.xml
COPY hbase/regionservers ${TEMPLATE_DIR}/regionservers
COPY hbase/hbase-env.sh ${TEMPLATE_DIR}/hbase-env.sh
COPY hdfs/mapred-site.xml ${TEMPLATE_DIR}/mapred-site.xml
COPY hdfs/yarn-site.xml ${TEMPLATE_DIR}/yarn-site.xml
COPY hdfs/yarn-env.sh ${TEMPLATE_DIR}/yarn-env.sh
COPY kafka/server.properties ${TEMPLATE_DIR}/server.properties
COPY solr/solr ${TEMPLATE_DIR}/solr

# COPY spark/spark-defaults.conf ${TEMPLATE_DIR}/spark-defaults.conf
#
# COPY spark2/spark2-shell ${TEMPLATE_DIR}/spark2/spark2-shell
# COPY spark2/spark2-submit ${TEMPLATE_DIR}/spark2/spark2-submit
# COPY spark2/spark-env.sh ${TEMPLATE_DIR}/spark2/spark-env.sh
# COPY spark2/spark-defaults.conf ${TEMPLATE_DIR}/spark2/spark-defaults.conf

COPY mongodb/mongod.conf ${TEMPLATE_DIR}/mongod.conf


RUN set -axe \
    && sudo -u zookeeper zookeeper-server-initialize --myid=1 --force \
    && sudo -u hdfs hdfs namenode -format -nonInteractive -force \
#     && sudo -u hdfs hadoop namenode -format -nonInteractive -force \
    && chmod +x /usr/bin/entrypoint.sh \
    && chown root:root /usr/bin/entrypoint.sh \
#     && mkdir -p /etc/spark2/conf \
    && chmod +x /usr/bin/resolve-templates.sh \
    && chown root:root /usr/bin/resolve-templates.sh \
    && bash /usr/bin/resolve-templates.sh \
    && service zookeeper-server start \
    && service hadoop-hdfs-namenode start \
    && service hadoop-hdfs-datanode start \
    && sudo -u hdfs hdfs dfs -mkdir /hbase \
    && sudo -u hdfs hdfs dfs -chown hbase:hbase /hbase \
    && sudo -u hdfs hdfs dfs -mkdir /user \
    && sudo -u hdfs hdfs dfs -chmod 1777 /user \
    && sudo -u hdfs hdfs dfs -chown mapred:hadoop /user \
    && sudo -u hdfs hdfs dfs -mkdir /user/history \
    && sudo -u hdfs hdfs dfs -chmod 1777 /user/history \
    && sudo -u hdfs hdfs dfs -chown mapred:hadoop /user/history \
    && sudo -u hdfs hdfs dfs -mkdir /tmp \
    && sudo -u hdfs hdfs dfs -chmod -R 1777 /tmp \
    && sudo -u hdfs hdfs dfs -mkdir -p hdfs:///var/log/hadoop-yarn/apps \
    && sudo -u hdfs hdfs dfs -chmod 1777 hdfs:///var/log/hadoop-yarn/apps \
    && sudo -u hdfs hdfs dfs -chown mapred:hadoop hdfs:///var/log/hadoop-yarn/apps \
    && sudo -u hdfs hdfs dfs -mkdir /solr \
    && sudo -u hdfs hdfs dfs -chown solr /solr \
    && hdfs dfs -mkdir -p /user/root/spark2/lib \
    && hdfs dfs -copyFromLocal /usr/lib/spark/jars/* /user/root/spark2/lib \
    && hdfs dfs -mkdir -p /user/root/nifi/bootstrap \
    && hdfs dfs -mkdir -p /user/root/nifi/system \
    && hdfs dfs -mkdir -p /user/root/nifi/extensions \
    && hdfs dfs -mkdir -p /user/root/nifi/stateless \
    && hdfs dfs -copyFromLocal /nifi-libs/stateless/* /user/root/nifi/stateless \
    && hdfs dfs -copyFromLocal /nifi-libs/bootstrap/* /user/root/nifi/bootstrap \
    && hdfs dfs -copyFromLocal /nifi-libs/system/* /user/root/nifi/system \
    && hdfs dfs -copyFromLocal /nifi-libs/extensions /user/root/nifi/ \
    && service hbase-master start \
    && service hbase-regionserver start \
    && sudo mkdir -p /usr/lib/hadoop-yarn/logs \
    && sudo chmod 777 /usr/lib/hadoop-yarn/logs \
    && service hadoop-yarn-resourcemanager start \
    && service hadoop-yarn-nodemanager start \
    && service hadoop-mapreduce-historyserver start \
    && service solr-server start \
    && solrctl init \
    && echo "create_namespace 'AVRO'" | hbase shell -n \
    && echo "create 'AVRO:SCHEMA_REPOSITORY', '0'" | hbase shell -n



COPY mongodb/mongod /etc/init.d/mongod

RUN set -axe \
    && wget https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org-mongos_3.4.8_amd64.deb \
    && wget https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org-server_3.4.8_amd64.deb \
    && wget https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org-shell_3.4.8_amd64.deb \
    && wget https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org-tools_3.4.8_amd64.deb \
    && wget https://repo.mongodb.org/apt/ubuntu/dists/xenial/mongodb-org/3.4/multiverse/binary-amd64/mongodb-org_3.4.8_amd64.deb \
    && dpkg --force-confold -i mongo*.deb \
    && chmod +x /etc/init.d/mongod

# NIFI
RUN set -axe \
    && wget --no-check-certificate https://archive.apache.org/dist/nifi/1.11.4/nifi-1.11.4-bin.tar.gz && tar -xzvf nifi-1.11.4-bin.tar.gz && rm nifi-1.11.4-bin.tar.gz && mv nifi-1.11.4 /nifi \
    && wget --no-check-certificate https://archive.apache.org/dist/nifi/nifi-registry/nifi-registry-0.6.0/nifi-registry-0.6.0-bin.tar.gz && tar -xzvf nifi-registry-0.6.0-bin.tar.gz && rm nifi-registry-0.6.0-bin.tar.gz && mv nifi-registry-0.6.0 /nifi-registry \
    && wget --no-check-certificate https://archive.apache.org/dist/nifi/1.11.4/nifi-toolkit-1.11.4-bin.tar.gz && tar -xzvf nifi-toolkit-1.11.4-bin.tar.gz && rm nifi-toolkit-1.11.4-bin.tar.gz && mv nifi-toolkit-1.11.4 /nifi-toolkit

RUN /nifi/bin/nifi.sh install
RUN /nifi-registry/bin/nifi-registry.sh install

RUN set -axe && \
curl -O http://packages.confluent.io/archive/6.0/confluent-6.0.0.tar.gz && \
tar xzvf confluent-6.0.0.tar.gz && \
cd confluent-6.0.0 && \
rm -rf ./share/java/ce-kafka-http-server/ && \
rm -rf ./share/java/ce-kafka-rest-extensions/ && \
rm -rf ./share/java/confluent-control-center/ && \
rm -rf ./share/java/confluent-hub-client/ && \
rm -rf ./share/java/confluent-kafka-mqtt/ && \
rm -rf ./share/java/confluent-metadata-service/ && \
rm -rf ./share/java/confluent-rebalancer/ && \
rm -rf ./share/java/kafka && \
rm -rf ./share/java/kafka-connect-replicator/ && \
rm -rf ./share/java/kafka-rest-bin/ && \
rm -rf ./share/java/kafka-rest-lib && \
rm -rf ./share/java/kafka-serde-tools/ && \
rm -rf ./share/java/ksqldb/ && \
rm -rf ./share/java/monitoring-interceptors/ && \
rm -rf ./share/java/acl/ && \
rm -rf ./share/doc/ && \
rm -rf ./share/java/confluent-security/connect && \
rm -rf ./share/java/confluent-security/kafka-rest && \
rm -rf ./share/java/confluent-security/ksql && \
rm -rf ./share/java/confluent-security/schema-validator && \
rm -rf ./libexec/ && \
rm -rf ./src && \
cd .. && \
rm -rf confluent-6.0.0.tar.gz


COPY schema-registry/schema-registry.properties ${TEMPLATE_DIR}/schema-registry.properties

RUN echo '\ncat ${TEMPLATE_DIR}/schema-registry.properties | envsubst \$HOSTNAME > /confluent-6.0.0/etc/schema-registry/schema-registry.properties' >> /usr/bin/resolve-templates.sh

CMD [ "/bin/bash", "/usr/bin/entrypoint.sh" ]