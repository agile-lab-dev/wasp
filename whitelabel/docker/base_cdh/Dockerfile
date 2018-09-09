FROM centos:7.5.1804
MAINTAINER AgileLab WASP Team <wasp@agilelab.it>

ENV TERM xterm

ENV JAVA_HOME /usr/java/jdk1.8.0_181-amd64/
ENV JAVA_VERSION 8b181
ARG CLOUDERA_VERSION=5.12

ADD cloudera-cdh5.repo.template ./

RUN \
        sed "s/{{ CLOUDERA_VERSION }}/${CLOUDERA_VERSION}/g" ./cloudera-cdh5.repo.template > cloudera-cdh5.repo  && \
        rm -f cloudera-cdh5.repo.template && \
        cat ./cloudera-cdh5.repo  && \
        yum install -y -q wget dialog curl sudo lsof vim axel telnet htop iotop && \
        wget --no-cookies -q --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u181-b13/96a7b8442fe848ef90c96a2fad6ed6d1/jdk-8u181-linux-x64.rpm && \
        yum localinstall -y jdk-8u181-linux-x64.rpm && \
        rm jdk-8u181-linux-x64.rpm && \
        echo "${JAVA_HOME}" >> /etc/environment && \
        echo 'Installing Cloudera repositories...' && \
        rpm --import https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera && \
        yum-config-manager --add-repo ./cloudera-cdh5.repo && \
        echo 'Cloudera repositories Installed.' && \
        yum install -y -q hadoop && \
        yum -y clean all

CMD ["bash"]
