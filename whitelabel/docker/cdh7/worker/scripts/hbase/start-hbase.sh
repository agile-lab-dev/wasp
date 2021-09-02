#!/bin/bash
source /usr/bin/bigtop-detect-javahome
set -eax
mkdir -p /var/log/hbase/
/opt/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/hbase/bin/hbase-daemon.sh --config /etc/hbase/conf/ start master
/opt/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/hbase/bin/hbase-daemon.sh --config /etc/hbase/conf/ start regionserver