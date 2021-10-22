FROM ubuntu:21.10  as cache

RUN apt-get update && apt-get -y install axel wget

RUN axel -o parcel-install.tgz -n 4 https://archive.cloudera.com/cdh7/7.1.7.0/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976-el7.parcel

RUN apt-get update && apt-get -y install tar gzip

RUN mkdir /parcel

RUN tar xzvf parcel-install.tgz --directory=/parcel --strip 1 

FROM golang:1.13.5 as build-env

WORKDIR /go/src/envsubst
COPY envsubst /go/src/envsubst

RUN go get -d -v ./...

RUN go build -o /go/bin/envsubst


FROM centos:centos7 

ENV BUILD_INSTALL_SCRIPTS_DIR="/opt/install-scripts" 

COPY install-scripts/common $BUILD_INSTALL_SCRIPTS_DIR/common

WORKDIR $BUILD_INSTALL_SCRIPTS_DIR

COPY install-scripts/install-prerequisites.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./install-prerequisites.sh 

COPY --from=cache /parcel/ /opt/parcel
COPY --from=build-env /go/bin/envsubst /usr/bin/genvsubst

COPY install-scripts/configure-cdp.sh $BUILD_INSTALL_SCRIPTS_DIR/configure-cdp.sh

RUN bash ./configure-cdp.sh

ENV HADOOP_COMMON_HOME=/usr/lib/hadoop/ \
    HADOOP_HDFS_HOME=/usr/lib/hadoop-hdfs/ \
    HADOOP_MAPRED_HOME=/lib/hadoop-mapreduce/ \
    HADOOP_YARN_HOME=/usr/lib/hadoop-yarn/ \
    HADOOP_CONF_DIR=/etc/hadoop/conf/ \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root \
    YARN_PROXYSERVER_USER=root

ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/parcel/bin"

ENV BUILD_TEMPLATES_DIR="/opt/templates" 

COPY templates/hadoop $BUILD_TEMPLATES_DIR/hadoop
COPY templates/resolve-hadoop-templates.sh $BUILD_TEMPLATES_DIR

ENV HADOOP_HEAPSIZE=256m

COPY install-scripts/configure-hadoop.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./configure-hadoop.sh

ENV SPARK_HOME=/usr/lib/spark
COPY templates/spark $BUILD_TEMPLATES_DIR/spark
COPY templates/resolve-spark-templates.sh $BUILD_TEMPLATES_DIR


ENV KAFKA_HOME=/usr/lib/kafka
COPY templates/kafka $BUILD_TEMPLATES_DIR/kafka
COPY templates/resolve-kafka-templates.sh $BUILD_TEMPLATES_DIR

ENV ZOOKEEPER_HOME=/usr/lib/zookeeper
COPY templates/zookeeper $BUILD_TEMPLATES_DIR/zookeeper
COPY templates/resolve-zookeeper-templates.sh $BUILD_TEMPLATES_DIR

ENV HBASE_HOME=/usr/lib/hbase
COPY templates/hbase $BUILD_TEMPLATES_DIR/hbase
COPY templates/resolve-hbase-templates.sh $BUILD_TEMPLATES_DIR

COPY templates/mongo/mongodb.repo /etc/yum.repos.d/mongodb.repo
COPY install-scripts/install-mongo.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./install-mongo.sh

COPY install-scripts/install-supervisord.sh $BUILD_INSTALL_SCRIPTS_DIR/
RUN exec bash ./install-supervisord.sh

ENV SOLR_HOME=/usr/lib/solr
COPY templates/solr $BUILD_TEMPLATES_DIR/solr
COPY templates/resolve-solr-templates.sh $BUILD_TEMPLATES_DIR

COPY runtime/entrypoint.sh /runtime/entrypoint.sh

ENTRYPOINT ["/bin/bash", "/runtime/entrypoint.sh"]