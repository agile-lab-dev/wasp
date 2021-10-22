
mkdir -p /env
mkdir -p /etc/solr/conf

echo "HOSTNAME=$HOSTNAME" > /env/hostname.env

genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/solr/conf --out=/etc/solr/conf
genvsubst --env /env --any --sub $BUILD_TEMPLATES_DIR/solr/bin --out=/usr/lib/solr/bin
