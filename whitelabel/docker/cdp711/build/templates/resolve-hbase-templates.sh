
mkdir -p /env
mkdir -p /etc/hbase/conf

echo "HOSTNAME=$HOSTNAME" > /env/hostname.env

genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/hbase --out=/etc/hbase/conf
