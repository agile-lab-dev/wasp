set -eax

while [[ $# -gt 0 ]]; do
    ARG=$1
    case $ARG in
        --interactive)
            INTERACTIVE=true
            shift # past argument
        ;;
    esac
done

/opt/resolve-templates.sh
/opt/scripts/hdfs/start-hdfs.sh
/opt/scripts/zookeeper/start-zookeeper.sh
/opt/scripts/kafka/start-kafka.sh
/opt/scripts/hbase/start-hbase.sh
/opt/scripts/yarn/start-yarn.sh
/opt/scripts/solr/start-solr.sh
/opt/scripts/mongo/start-mongo.sh

if [ "$INTERACTIVE" = true ]
 then
    bash
fi