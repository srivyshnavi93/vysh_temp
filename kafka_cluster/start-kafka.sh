#!/bin/bash

if [ "$KAFKA_WORKLOAD" == "zookeeper" ];
then
    $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

elif [ "$KAFKA_WORKLOAD" == "broker" ];
then
    ERR_MSG="E: If either ZOOKEEPER_HOST or ZOOKEEPER_PORT is defined, both must be defined"
    if [ -n "${ZOOKEEPER_HOST}" ] || [ -n "${ZOOKEEPER_PORT}" ]; then
      if [ -z "${ZOOKEEPER_HOST}" ]; then
        echo "E: ZOOKEEPER_HOST is not defined"
        echo "${ERR_MSG}"
        exit 1
      elif [ -z "${ZOOKEEPER_PORT}" ]; then
        echo "E: ZOOKEEPER_PORT is not defined"
        echo "${ERR_MSG}"
        exit 1
      fi
      exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties --override zookeeper.connect="${ZOOKEEPER_HOST}":"${ZOOKEEPER_PORT}"
    else
      exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
    fi

else
    echo "Undefined Workload Type $KAFKA_WORKLOAD, must specify: zookeeper, broker"

fi    