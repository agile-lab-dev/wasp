cat ${BUILD_TEMPLATES_DIR}/polynote/config.yml | envsubst \$HOSTNAME > $POLYNOTE_HOME/config.yml

