mkdir -p /env
mkdir -p /etc/spark/conf

echo "HOSTNAME=$HOSTNAME" > /env/hostname.env

genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/spark --out=/etc/spark/conf
