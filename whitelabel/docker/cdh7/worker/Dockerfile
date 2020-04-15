FROM registry.gitlab.com/agilefactory/agile.wasp2/cdh7:base

COPY templates/mongo/mongodb.repo /etc/yum.repos.d/mongodb.repo

RUN yum install -y --nogpgcheck epel-release
RUN yum install -y --nogpgcheck  gettext lsof mongodb-org supervisor

RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/lib | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/lib/"$1" /usr/lib/"$1}' | bash 
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/bin | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/bin/"$1" /usr/bin/"$1}' | bash 
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/default | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/default/"$1" /etc/default/"$1}' | bash 
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/hadoop | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/default/"$1" /etc/default/"$1}' | bash 

RUN mkdir -p /etc/hadoop/conf
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/hadoop/conf.pseudo | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/hadoop/conf.pseudo/"$1" /etc/hadoop/conf/"$1}' | bash 
RUN mkdir -p /var/lib/hadoop-hdfs/cache/root/dfs/name
RUN mkdir -p /var/run/hadoop-hdfs

RUN mkdir -p /etc/zookeeper/conf
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/zookeeper/conf.dist | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/zookeeper/conf.dist/"$1" /etc/zookeeper/conf/"$1}' | bash 


RUN mkdir -p /etc/kafka/conf
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/kafka/conf.dist | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/kafka/conf.dist/"$1" /etc/kafka/conf/"$1}' | bash 

RUN mkdir -p /etc/hbase/conf
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/hbase/conf.dist | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/hbase/conf.dist/"$1" /etc/hbase/conf/"$1}' | bash 

RUN mkdir -p /etc/spark/conf
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/spark/conf.dist | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/spark/conf.dist/"$1" /etc/spark/conf/"$1}' | bash 

RUN mkdir -p /etc/solr/conf
RUN ls -1 /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/solr/conf.dist | awk '{print "ln -s /opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/etc/solr/conf.dist/"$1" /etc/solr/conf/"$1}' | bash 


COPY scripts /opt/scripts
COPY templates /opt/templates
COPY resolve-templates.sh /opt

RUN chmod +x -R /opt/scripts

RUN /opt/resolve-templates.sh \
    && /opt/scripts/hdfs/initialize-hdfs.sh \
    && /opt/scripts/zookeeper/initialize-zookeeper.sh

CMD /opt/scripts/start-all.sh --interactive