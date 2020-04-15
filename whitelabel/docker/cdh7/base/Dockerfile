FROM centos/systemd

ENV CLOUDERA_REPO_URL https://archive.cloudera.com/cm7/7.0.3/redhat7/yum/

# ADD CDH7 CLOUDERA RPM REPOSITORY
RUN yum-config-manager --add-repo $CLOUDERA_REPO_URL
RUN yum install --nogpgcheck -y openssh-server which openssh-clients ntp openjdk8

# CONFIGURE SSH
RUN rm -rf /etc/ssh/ssh_host*
COPY services/ssh/ssh-host-key.service /etc/systemd/system/
RUN chmod 664 /etc/systemd/system/ssh-host-key.service
RUN systemctl enable ssh-host-key.service
COPY services/ssh/sshd_config /etc/ssh/sshd_config

# ENABLE LOGIN VIA USERNAME PASSWORD
RUN echo "root:root" | chpasswd 

RUN mkdir /root/.ssh && \
  touch /root/.ssh/authorized_keys && \
  chmod 700 /root/.ssh && \
  chmod 600 /root/.ssh/authorized_keys

ADD cache/CDH-7.0.3-1.cdh7.0.3.p0.1635019-el7.parcel.tar.gz /opt/parcels

EXPOSE 22

CMD ["/usr/sbin/init"]