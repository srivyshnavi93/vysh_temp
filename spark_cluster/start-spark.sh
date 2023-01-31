#!/bin/bash

. "$SPARK_HOME/bin/load-spark-env.sh"

# If the spark work_load is master, run master
if [ "$SPARK_WORKLOAD" == "master" ];
then
    mkdir -p $SPARK_HOME/shared-workspace/spark/logs/master-logs
    touch $SPARK_HOME/shared-workspace/spark/logs/master-logs/spark-master.out
    $SPARK_HOME/sbin/start-master.sh >> $SPARK_HOME/shared-workspace/spark/logs/master-logs/spark-master.out 

# If the spark work_load is worker, run worker
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
    mkdir -p $SPARK_HOME/shared-workspace/spark/logs/worker-logs
    touch $SPARK_HOME/shared-workspace/spark/logs/worker-logs/spark-worker.out
    $SPARK_HOME/sbin/start-worker.sh spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> $SPARK_HOME/shared-workspace/spark/logs/worker-logs/spark-worker.out

# If the spark work_load is history, run history-server
elif [ "$SPARK_WORKLOAD" == "history" ];
then
    mkdir -p $SPARK_HOME/shared-workspace/spark/logs/spark-hist-logs
    $SPARK_HOME/sbin/start-history-server.sh

# If the spark work_load is jupyter, run jupyter-lab
elif [ "$SPARK_WORKLOAD" == "jupyter" ];
then
    jupyter lab --no-browser --allow-root --ip 0.0.0.0 --NotebookApp.token='' 

elif [ "$SPARK_WORKLOAD" == "submit" ];
then
    echo "SPARK SUBMIT"

else
    echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, history, submit"

fi