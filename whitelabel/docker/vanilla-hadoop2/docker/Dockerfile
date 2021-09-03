FROM ubuntu:16.04

ENV BUILD_INSTALL_SCRIPTS_DIR="/opt/install-scripts" 

COPY install-scripts/common $BUILD_INSTALL_SCRIPTS_DIR/common

WORKDIR $BUILD_INSTALL_SCRIPTS_DIR

COPY install-scripts/install-prerequisites.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./install-prerequisites.sh 

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/opt/hadoop/logs \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root \
    YARN_PROXYSERVER_USER=root \
    YARN_CONF_DIR=$HADOOP_CONF_DIR \
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HADOOP_COMMON_HOME=/opt/hadoop \
    HADOOP_HDFS_HOME=/opt/hadoop \
    YARN_HOME=/opt/hadoop

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

COPY install-scripts/install-hadoop.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./install-hadoop.sh 

ENV BUILD_TEMPLATES_DIR="/opt/templates" 

COPY templates $BUILD_TEMPLATES_DIR/

ENV HADOOP_HEAPSIZE=256

COPY install-scripts/install-spark.sh $BUILD_INSTALL_SCRIPTS_DIR/
ENV SPARK_HOME=/opt/spark
RUN exec bash ./install-spark.sh

COPY install-scripts/configure-spark.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./configure-spark.sh

COPY install-scripts/configure-hadoop.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./configure-hadoop.sh

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

COPY install-scripts/install-kafka.sh $BUILD_INSTALL_SCRIPTS_DIR/
ENV KAFKA_HOME=/opt/kafka
RUN exec bash ./install-kafka.sh

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$KAFKA_HOME/bin


COPY install-scripts/install-hbase.sh $BUILD_INSTALL_SCRIPTS_DIR/
ENV HBASE_HOME=/opt/hbase
RUN exec bash ./install-hbase.sh

ENV HBASE_MANAGES_ZK=false

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$KAFKA_HOME/bin:$HBASE_HOME/bin

COPY install-scripts/install-mongo.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./install-mongo.sh

COPY install-scripts/install-solr.sh $BUILD_INSTALL_SCRIPTS_DIR/
ENV SOLR_HOME=/opt/solr
RUN exec bash ./install-solr.sh

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$KAFKA_HOME/bin:$HBASE_HOME/bin:$SOLR_HOME/bin

COPY runtime/entrypoint.sh /runtime/entrypoint.sh

ENV HBASE_CONF_DIR=/opt/hbase/conf

ENTRYPOINT ["/bin/bash", "/runtime/entrypoint.sh"]