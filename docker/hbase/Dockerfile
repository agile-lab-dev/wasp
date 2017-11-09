# VERSION   0.1

FROM base_cdh:5.11
MAINTAINER Mattia Bertorello <mattia.bertorello@agilelab.it>

ADD ./hbase-server /opt/hbase-server

RUN yum -y  install hbase hbase-thrift hbase-master hbase-regionserver && \
    echo "export HBASE_MANAGES_ZK=true" >> /etc/hbase/conf.dist/hbase-env.sh && \
    chmod +x /opt/hbase-server && \
    yum -y clean all

ADD ./replace-hostname /opt/replace-hostname
COPY conf  /etc/hadoop/conf/
ADD ./hbase-site.xml /etc/hbase/conf/hbase-site.xml
ADD ./zoo.cfg /etc/hbase/conf/zoo.cfg


VOLUME /data

####################
# PORTS
####################
# 8080 - REST API
# 8085 - REST Web UI at :8085/rest.jsp
# 9090 - Thrift API
# 9095 - Thrift Web UI at :9095/thrift.jsp
# 2181 - HBase's Embedded zookeeper cluster
# 60010 - HBase Master web UI at :6010/master-status;  ZK at :16010/zk.jsp

EXPOSE 8080 8085 9090 9095 2181 60010

CMD ["/opt/hbase-server"]