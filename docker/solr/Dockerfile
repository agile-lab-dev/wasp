# VERSION   0.1

FROM base_cdh:5.11

MAINTAINER Mattia Bertorello <mattia.bertorello@agilelab.it>


RUN yum -y install solr-server && \
    mkdir -p /data && \
    yum -y clean all


#The solr config file needs to be added after installation or it fails.
ADD solr /etc/default/solr.docker

ADD docker-entrypoint.sh /opt/docker-solr/docker-entrypoint.sh
ADD hadoop-env.sh /etc/profile.d/hadoop-env.sh
COPY conf  /etc/hadoop/conf/

VOLUME /data

ADD solr-foreground /opt/docker-solr/solr-foreground


RUN  mv /etc/default/solr.docker /etc/default/solr && \
    chmod +x /opt/docker-solr/solr-foreground

ADD conf/solr.xml /usr/lib/solr/clusterconfig/solr.xml
ADD conf/solrconfig.xml /usr/lib/solr/coreconfig-template/solrconfig.xml
ADD conf/solrconfig.xml /usr/lib/solr/managedTemplate/solrconfig.xml
ADD conf/solrconfig.xml /usr/lib/solr/predefinedTemplate/solrconfig.xml
ADD conf/solrconfig.xml /usr/lib/solr/schemalessTemplate/solrconfig.xml

####################
# PORTS
####################
EXPOSE 8983

WORKDIR /opt/docker-solr/
ENTRYPOINT ["/bin/bash", "/opt/docker-solr/docker-entrypoint.sh"]
CMD ["/bin/bash", "/opt/docker-solr/solr-foreground"]