FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

COPY mongo.repo /etc/yum.repos.d/mongo.repo

RUN microdnf install shadow-utils findutils --nodocs \
    && groupadd -r mongodb && useradd -r -g mongodb mongodb \
    && microdnf install --enablerepo=mongodb-org-4.2 mongodb-org-server --nodocs \
    && microdnf clean all

# grab gosu for easy step-down from root (https://github.com/tianon/gosu/releases)
ENV GOSU_VERSION 1.12
# grab "js-yaml" for parsing mongod's YAML config files (https://github.com/nodeca/js-yaml/releases)
ENV JSYAML_VERSION 3.13.1
ENV JQ_VERSION 1.6

RUN curl -L "https://github.com/tianon/gosu/releases/download/${GOSU_VERSION}/gosu-amd64" -o /bin/gosu \
	&& curl -L -o /js-yaml.js "https://github.com/nodeca/js-yaml/raw/${JSYAML_VERSION}/dist/js-yaml.js" \
    && curl -L -o /bin/jq "https://github.com/stedolan/jq/releases/download/jq-$JQ_VERSION/jq-linux64" \
    && chmod +x /bin/gosu \
    && chmod +x /bin/jq
 
RUN mkdir -p /data/db /data/configdb \
	&& chown -R mongodb:mongodb /data/db /data/configdb

VOLUME /data/db /data/configdb

COPY docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

EXPOSE 27017

COPY mongodb.txt /licenses/mongodb.txt

LABEL name="wasp-db"
LABEL vendor="AgileLab s.r.l."
LABEL version="2.26-SNAPSHOT"
LABEL release="1"
LABEL summary="WASP is a framework to build complex real time big data applications. It relies on Kappa/Lambda architecture leveraging Kafka and Spark."
LABEL description="WASP is a big data framework that allows you to not waste time with devops architectures and integrating different components. WASP lets you focus on your data, business logic and algorithms, without worrying about tipical big data problems like: at least once or exactly once delivery,periodically training a machine learning model, publishing your results in real time, to be reactive,applying schemas to unstructured data,feeding different datastores from the same data flow in a safe way"

CMD ["mongod"]