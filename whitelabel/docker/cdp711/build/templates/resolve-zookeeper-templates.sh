mkdir -p /env
mkdir -p /etc/zookeeper/conf

echo "HOSTNAME=$HOSTNAME" > /env/hostname.env

genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/zookeeper --out=/etc/zookeeper/conf
