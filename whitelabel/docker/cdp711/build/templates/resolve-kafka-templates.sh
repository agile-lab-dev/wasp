mkdir -p /env
mkdir -p /etc/kafka/conf
mkdir -p /etc/kafka2/conf

echo "HOSTNAME=$HOSTNAME" > /env/hostname.env

genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/kafka --out=/etc/kafka/conf
genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/kafka2 --out=/etc/kafka2/conf