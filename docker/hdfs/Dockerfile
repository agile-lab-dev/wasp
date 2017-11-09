# VERSION   0.1

FROM base_cdh:5.11
MAINTAINER Mattia Bertorello <mattia.bertorello@agilelab.it>

RUN yum -y  -q install hadoop-conf-pseudo && \
    yum -y clean all


ADD hadoop-env.sh /etc/profile.d/hadoop-env.sh
ADD docker-entrypoint.sh /root/docker-entrypoint.sh

COPY conf  /etc/hadoop/conf/

# Formatting HDFS
RUN mkdir -p /data/dfs/data /data/dfs/name /data/dfs/namesecondary && \
    touch /tmp/namenodeformat
VOLUME /data

####################
# PORTS
####################
#
# http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.3.0/bk_HDP_Reference_Guide/content/reference_chap2.html
# http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_ig_ports_cdh5.html
# http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/core-default.xml
# http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml

# HDFS: NameNode (NN):
#	 8020 = fs.defaultFS			(IPC / File system metadata operations)
#						(9000 is also frequently used alternatively)
#	 8022 = dfs.namenode.servicerpc-address	(optional port used by HDFS daemons to avoid sharing RPC port)
#       50070 = dfs.namenode.http-address	(HTTP  / NN Web UI)
#	50470 = dfs.namenode.https-address	(HTTPS / Secure UI)
# HDFS: DataNode (DN):
#	50010 = dfs.datanode.address		(Data transfer)
#	50020 = dfs.datanode.ipc.address	(IPC / metadata operations)
#	50075 = dfs.datanode.http.address	(HTTP  / DN Web UI)
#	50475 = dfs.datanode.https.address	(HTTPS / Secure UI)
# HDFS: Secondary NameNode (SNN)
#	50090 = dfs.secondary.http.address	(HTTP / Checkpoint for NameNode metadata)
EXPOSE 9000 50070 50010 50020 50075 50090

ENTRYPOINT ["/root/docker-entrypoint.sh"]
CMD ["hdfs"]