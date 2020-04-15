#!/bin/bash
source /usr/bin/bigtop-detect-javahome
set -eax
mkdir -p /var/log/hbase/
/opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/lib/hbase/bin/hbase-daemon.sh --config /etc/hbase/conf/ start master
/opt/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/lib/hbase/bin/hbase-daemon.sh --config /etc/hbase/conf/ start regionserver