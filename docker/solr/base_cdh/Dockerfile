# VERSION   0.1

FROM centos:7.3.1611
MAINTAINER Mattia Bertorello <mattia.bertorello@agilelab.it>


ENV TERM xterm

ENV JAVA_HOME /usr/java/jdk1.8.0_131
ENV JAVA_VERSION 8b131
ARG CLOUDERA_VERSION=5.11

ADD cloudera-cdh5.repo.template ./

RUN \
        sed "s/{{ CLOUDERA_VERSION }}/${CLOUDERA_VERSION}/g" ./cloudera-cdh5.repo.template > cloudera-cdh5.repo  && \
        rm -f cloudera-cdh5.repo.template && \
        cat ./cloudera-cdh5.repo  && \
        yum install -y -q wget dialog curl sudo lsof vim axel telnet htop iotop && \
        wget --no-cookies -q --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.rpm && \
        yum localinstall -y jdk-8u131-linux-x64.rpm && \
        rm jdk-8u131-linux-x64.rpm && \
        echo "${JAVA_HOME}" >> /etc/environment && \
        echo 'Installing Cloudera repositories...' && \
        rpm --import https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera && \
        yum-config-manager --add-repo ./cloudera-cdh5.repo && \
        echo 'Cloudera repositories Installed.' && \
        yum install -y -q hadoop && \
        echo "Add an HDH user and include as sudoer" && \
        useradd hdh && \
        echo "hdh:hdh" | chpasswd && \
        usermod -aG wheel hdh && \
        mkdir -p /home/hdh && \
        chown hdh:hdh /home/hdh && \
        echo "Complete add HDH user" && \
        yum -y clean all



# Define default command.
#CMD ["/usr/bin/cdh_startup_script.sh && bash"]
#CMD ["bash /usr/bin/cdh_startup_script.sh && bash"]
CMD ["bash"]
