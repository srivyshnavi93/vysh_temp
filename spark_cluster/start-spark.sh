#!/bin/bash
echo $SPARK_MASTER_HOST
. "${SPARK_HOME}/bin/load-spark-env.sh"

echo $SPARK_MASTER_HOST
# When the spark work_load is master run class org.apache.spark.deploy.master.Master
if [ "$SPARK_WORKLOAD" == "master" ];
then

/opt/spark/sbin/start-master.sh >> /opt/spark/logs/spark-master.out

elif [ "$SPARK_WORKLOAD" == "worker" ];
then
# When the spark work_load is worker run class org.apache.spark.deploy.master.Worker
/opt/spark/sbin/start-worker.sh spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> /opt/spark/logs/spark-worker.out

elif [ "$SPARK_WORKLOAD" == "submit" ];
then
    echo "SPARK SUBMIT"
else
    echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, submit"
fi