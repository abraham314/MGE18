#!/bin/bash

HOSTS_FILE_TMP=/tmp/hosts

MASTER_HOST=${MASTER_HOST:-master}
MASTER_IP=${MASTER_IP:-0.0.0.0}
NODE_TYPE=${NODE_TYPE:-node}
SLAVES=${SLAVES:-localhost}
NODE_MANAGER_WEB_PORT=${NODE_MANAGER_WEB_PORT:-8042}

sed -i -e "s|@MASTER_IP@|${MASTER_HOST}|g" ${HADOOP_CONF_DIR}/yarn-site.xml
sed -i -e "s|@NODE_MANAGER_WEB_PORT@|${NODE_MANAGER_WEB_PORT}|g" ${HADOOP_CONF_DIR}/yarn-site.xml
sed -i -e "s|@MASTER_IP@|${MASTER_HOST}|g" ${HADOOP_CONF_DIR}/core-site.xml

if [ -f "$HOSTS_FILE_TMP" ]; then
    cat $HOSTS_FILE_TMP >> /etc/hosts
fi

echo "${MASTER_IP} ${MASTER_HOST}" >> /etc/hosts

service ssh start

if [ ${NODE_TYPE} == "master" ]; then
    echo "" > ${HADOOP_CONF_DIR}/slaves
    hosts=(${SLAVES//,/ })
    for i in "${hosts[@]}"
        do
            : 
            echo $i >> ${HADOOP_CONF_DIR}/slaves
    done
    echo "Formating namenode..."
    $HADOOP_PREFIX/bin/hdfs namenode -format
    echo "Starting HDFS..."
    $HADOOP_PREFIX/sbin/start-dfs.sh
    echo "Starting YARN..."
    $HADOOP_PREFIX/sbin/start-yarn.sh
elif [ ${NODE_TYPE} == "slave" ]; then
    $HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode
    $HADOOP_PREFIX/sbin/yarn-daemon.sh start nodemanager
fi

supervisord -n



