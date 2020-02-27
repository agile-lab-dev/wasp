set -eax

mkdir -p /var/lib/mongo/data
mkdir -p /var/log/mongo/

mongod --port 27017  --bind_ip `hostname` --dbpath "/var/lib/mongo/data" &> /var/log/mongo/mongod.log & 
