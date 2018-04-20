#!/usr/bin/env bash

#The key must stay in the local directory because the sasl.jaas.config will be use also in the spark executors where It is in his local directory
#SCRIPT_DIR=
#KEYTAB_FILE_NAME=""
#REALM_SERVER=
#REALM=
#PRINCIPAL_NAME=""
#ETC_HOSTS=""
# example
#ETC_HOSTS="--add-host=server01.cluster01.atscom.it:192.168.69.230 --add-host=server02.cluster01.atscom.it:192.168.69.229 --add-host=server03.cluster01.atscom.it:192.168.69.228 --add-host=server04.cluster01.atscom.it:192.168.69.227 --add-host=server05.cluster01.atscom.it:192.168.69.226 --add-host=server06.cluster01.atscom.it:192.168.69.225 --add-host=server07.cluster01.atscom.it:192.168.69.224 --add-host=server08.cluster01.atscom.it:192.168.69.223"
#SCRIPT_DIR="$(pwd)"
#KEYTAB_FILE_NAME=./wasp2.keytab
#REALM_SERVER=server08.cluster01.atscom.it
#REALM=CLUSTER01.ATSCOM.IT
#PRINCIPAL_NAME=wasp2@${REALM}

cat <<EOT > ${SCRIPT_DIR}/krb5.conf
[libdefaults]
default_realm = ${REALM}
dns_lookup_kdc = false
dns_lookup_realm = false
ticket_lifetime = 86400
renew_lifetime = 604800
forwardable = true
default_tgs_enctypes = arcfour-hmac
default_tkt_enctypes = arcfour-hmac
permitted_enctypes = arcfour-hmac
udp_preference_limit = 1
kdc_timeout = 3000

[realms]
${REALM} = {
  kdc = ${REALM_SERVER}
  admin_server = ${REALM_SERVER}
}
EOT

cat <<EOT > ${SCRIPT_DIR}/sasl.jaas.config
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  useTicketCache=true
  keyTab="${KEYTAB_FILE_NAME}"
  principal="${PRINCIPAL_NAME}";
};
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  useTicketCache=true
  keyTab="${KEYTAB_FILE_NAME}"
  serviceName="kafka"
  principal="${PRINCIPAL_NAME}";
};
EOT