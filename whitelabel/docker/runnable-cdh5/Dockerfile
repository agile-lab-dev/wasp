FROM registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:5.16

RUN mkdir -p /code

COPY consumers-spark/target/universal/stage /code/consumers-spark
COPY master/target/universal/stage /code/master
COPY producers/target/universal/stage /code/producer

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY log4j2.properties /log4j2.properties
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY templates/hbase-site.xml /templates/hbase-site.xml
COPY docker-environment.conf /wasp.conf


ENV MASTER_MEMORY 512m
ENV PRODUCERS_MEMORY 512m
ENV SPARK_STREAMING_MEMORY 512m
ENV SPARK_BATCH_MEMORY 512m

CMD [ "/bin/bash", "/docker-entrypoint.sh" ]
