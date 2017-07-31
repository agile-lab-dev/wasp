#
# Oracle Java 8 Dockerfile
#
# https://github.com/dockerfile/java
# https://github.com/dockerfile/java/tree/master/oracle-java8
#

# Pull base image.
FROM library/ubuntu:16.04

# Java Version
ENV JAVA_VERSION 8u162

# Install Java.

MAINTAINER AgileLab <infra@agilelab.it>

ENV JAVA_PKG=jre-8u*-linux-x64.tar.gz \
    JAVA_HOME=/usr/java/default

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y curl wget tcpdump iputils-ping krb5-kdc krb5-admin-server python gettext-base && \
    mkdir /usr/java/ && \
    echo "Start downloading oracle jre ${JAVA_VERSION}" && \
    curl -C - -L -O -R -s -S -H "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u162-b12/0da788060d494f5095bf8624735fa2f1/jre-8u162-linux-x64.tar.gz && \
    tar -xvzf ${JAVA_PKG}  -C /usr/java/ && \
    rm -rf ${JAVA_PKG} && \
    export JAVA_DIR=$(ls -1 -d /usr/java/*) && \
    ln -s $JAVA_DIR /usr/java/latest && \
    ln -s $JAVA_DIR /usr/java/default && \
    update-alternatives --install /usr/bin/java java $JAVA_DIR/bin/java 20000 && \
    apt-get autoremove -y && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/oracle-jdk8-installer

# Define working directory.
WORKDIR /root

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/java/default

# Define default command.
CMD ["bash"]