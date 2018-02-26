#!/usr/bin/env bash
set -e
echo "$@"
if [ -n "$WASP_SECURITY" ]; then
    echo $KRB5_CONFIG $KEYTAB_FILE_NAME $PRINCIPAL_NAME
    ls -lha $KRB5_CONFIG
    cat $KRB5_CONFIG
    kinit -V -k -t /root/configurations/$KEYTAB_FILE_NAME $PRINCIPAL_NAME
fi
exec "$@"