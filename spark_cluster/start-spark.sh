#!/bin/bash

. "$SPARK_HOME/bin/load-spark-env.sh"

# If the spark work_load is master, run master
if [ "$SPARK_WORKLOAD" == "master" ];
then
    mkdir -p $SPARK_HOME/work-dir/odp_intra_storage/spark/logs/master
    touch $SPARK_HOME/work-dir/odp_intra_storage/spark/logs/master/spark-master.out
    $SPARK_HOME/sbin/start-master.sh >> $SPARK_HOME/work-dir/odp_intra_storage/spark/logs/master/spark-master.out

# If the spark work_load is worker, run worker
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
    mkdir -p $SPARK_HOME/work-dir/odp_intra_storage/spark/logs/worker
    touch $SPARK_HOME/work-dir/odp_intra_storage/spark/logs/worker/spark-worker.out
    $SPARK_HOME/sbin/start-worker.sh spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> $SPARK_HOME/work-dir/odp_intra_storage/spark/logs/worker/spark-worker.out

# If the spark work_load is history, run history-server
elif [ "$SPARK_WORKLOAD" == "history" ];
then
    mkdir -p $SPARK_HOME/work-dir/odp_intra_storage/spark/logs/history-server/spark-hist-logs
    $SPARK_HOME/sbin/start-history-server.sh

# If the spark work_load is thrift, run thrift-server
elif [ "$SPARK_WORKLOAD" == "thrift" ];
then
    $SPARK_HOME/sbin/start-thriftserver.sh    

# If the spark work_load is jupyter, run jupyter-lab
elif [ "$SPARK_WORKLOAD" == "jupyter" ];
then
    jupyter lab --no-browser --allow-root --ip 0.0.0.0 --NotebookApp.token='' 


# If the spark work_load is submit, run nothing
elif [ "$SPARK_WORKLOAD" == "submit" ];
then
    echo "SPARK SUBMIT"

else
    echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, history, submit"

fi