mkdir -p /env
mkdir -p /etc/kafka/conf

echo "HOSTNAME=$HOSTNAME" > /env/hostname.env

genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/kafka --out=/etc/kafka/conf
