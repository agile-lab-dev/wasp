
mkdir -p /env
mkdir -p /etc/hadoop/conf

echo "HOSTNAME=$HOSTNAME" > /env/hostname.env

genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/hadoop --out=/etc/hadoop/conf
