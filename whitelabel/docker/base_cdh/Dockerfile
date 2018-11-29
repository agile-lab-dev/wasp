FROM centos:7.5.1804
MAINTAINER AgileLab WASP Team <wasp@agilelab.it>

ENV TERM xterm

ENV JAVA_HOME /usr/java/jdk1.8.0_191-amd64/
ARG CLOUDERA_VERSION=5.12


ENV JAVA_VERSION=8u191

ENV JAVA_PKG="jdk-${JAVA_VERSION}-linux-x64.rpm"


ENV JAVA_PKG_DOWNLOAD_URL="https://download.oracle.com/otn-pub/java/jdk/8u191-b12/2787e4a523244c269598db4e85c51e0c/$JAVA_PKG"


ADD cloudera-cdh5.repo.template ./

RUN \
        sed "s/{{ CLOUDERA_VERSION }}/${CLOUDERA_VERSION}/g" ./cloudera-cdh5.repo.template > cloudera-cdh5.repo  && \
        rm -f cloudera-cdh5.repo.template && \
        cat ./cloudera-cdh5.repo  && \
        yum install -y -q wget dialog curl sudo lsof vim axel telnet htop iotop && \
        wget --no-cookies -q --no-check-certificate --header "Cookie: gpw_e24=https%3A%2F%2Fwww.oracle.com%2Ftechnetwork%2Fjava%2Fjavase%2Fdownloads%2Fjdk8-downloads-2133151.html; oraclelicense=accept-securebackup-cookie" ${JAVA_PKG_DOWNLOAD_URL} && \   
        yum localinstall -y $JAVA_PKG && \
        rm $JAVA_PKG && \
        echo "${JAVA_HOME}" >> /etc/environment && \
        echo 'Installing Cloudera repositories...' && \
        rpm --import https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera && \
        yum-config-manager --add-repo ./cloudera-cdh5.repo && \
        echo 'Cloudera repositories Installed.' && \
        yum install -y -q hadoop && \
        yum -y clean all

CMD ["bash"]
