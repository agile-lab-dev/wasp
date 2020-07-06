FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

RUN microdnf install java-1.8.0-openjdk --nodocs \
    && microdnf clean all

RUN curl -fLo cs https://git.io/coursier-cli-linux \
    && chmod +x cs


COPY coursier.exclusions /coursier.exclusions
 
RUN ./cs fetch --classpath it.agilelab::wasp-producers:2.26.0-SNAPSHOT \
                           org.apache.logging.log4j:log4j-api:2.9.1 \
                           org.apache.logging.log4j:log4j-api:2.9.1 \
                           org.apache.logging.log4j:log4j-core:2.9.1 \
                           org.apache.logging.log4j:log4j-slf4j-impl:2.9.1  \
                           it.agilelab::darwin-hbase-connector:1.0.8 \
                           org.apache.hbase:hbase-client:2.1.0-cdh6.3.2 \
                           org.apache.hbase:hbase-common:2.1.0-cdh6.3.2 \
                           org.apache.hbase:hbase-server:2.1.0-cdh6.3.2 \
                            --repository https://oss.jfrog.org/artifactory/oss-snapshot-local \
                           --repository https://repository.cloudera.com/artifactory/cloudera-repos/ \
                           --local-exclude-file /coursier.exclusions \
                           > /wasp.classpath \
    && microdnf clean all

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY docker-environment.conf /docker-environment.conf
COPY log4j2.properties /log4j2.properties
COPY etc/hbase/conf/hbase-site.xml /etc/hbase/conf/hbase-site.xml

CMD [ "/bin/bash", "/docker-entrypoint.sh" ]
