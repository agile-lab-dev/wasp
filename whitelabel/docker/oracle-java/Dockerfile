#
# Oracle Java 8 Dockerfile
#
# https://github.com/dockerfile/java
# https://github.com/dockerfile/java/tree/master/oracle-java8
#
FROM library/ubuntu:16.04

MAINTAINER AgileLab <infra@agilelab.it>

# Define helpful variables
ENV JAVA_VERSION 8u191
ENV JAVA_PKG="jdk-${JAVA_VERSION}-linux-x64.tar.gz"


ENV JAVA_PKG_DOWNLOAD_URL="https://download.oracle.com/otn-pub/java/jdk/8u191-b12/2787e4a523244c269598db4e85c51e0c/$JAVA_PKG"

# Prepare, download, extract, configure, cleanup
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y curl wget tcpdump iputils-ping krb5-kdc krb5-admin-server python gettext-base && \
    mkdir /usr/java/ && \
    echo "Downloading Oracle JDK from ${JAVA_PKG_DOWNLOAD_URL}" && \
    wget --no-cookies -q --no-check-certificate --header "Cookie: gpw_e24=https%3A%2F%2Fwww.oracle.com%2Ftechnetwork%2Fjava%2Fjavase%2Fdownloads%2Fjdk8-downloads-2133151.html; oraclelicense=accept-securebackup-cookie" ${JAVA_PKG_DOWNLOAD_URL} && \
    echo "Extracting archive ${JAVA_PKG}" && \
    tar -xvzf ${JAVA_PKG}  -C /usr/java/ && \
    echo "Configuring" && \
    export JAVA_DIR=$(ls -1 -d /usr/java/*) && \
    ln -s $JAVA_DIR /usr/java/latest && \
    ln -s $JAVA_DIR /usr/java/default && \
    update-alternatives --install /usr/bin/java java $JAVA_DIR/bin/java 20000 && \
    echo "Cleaning up" && \
    rm ${JAVA_PKG} && \
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
