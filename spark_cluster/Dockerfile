ARG SPARK_VERSION=3.3.1

FROM apache/spark-py:v${SPARK_VERSION}

USER 0
RUN apt-get update && apt-get install -y curl git wget nano sudo

ENV SPARK_MASTER_HOST spark-master \
SPARK_MASTER_PORT 7077 \
PYSPARK_PYTHON python3

ARG SPARK_MASTER_WEB_UI=8080 \
SPARK_WORKER_WEB_UI=8081 \
SPARK_HISTORY_SERVER_WEB_UI=18080 \
JUPYTER_LAB_WEB_UI=8888

EXPOSE ${SPARK_MASTER_WEB_UI} \
${SPARK_MASTER_PORT} \
${SPARK_WORKER_WEB_UI} \
${SPARK_HISTORY_SERVER_WEB_UI} \
${JUPYTER_LAB_WEB_UI}

RUN mkdir /opt/spark/logs \
&& touch /opt/spark/logs/spark-master.out \
&& touch /opt/spark/logs/spark-worker.out \
&& ln -sf /dev/stdout /opt/spark/logs/spark-master.out \
&& ln -sf /dev/stdout /opt/spark/logs/spark-worker.out

COPY requirements.txt /
COPY conf /opt/spark/conf

COPY start-spark.sh /opt/spark/start-spark.sh
CMD ["/bin/bash", "/opt/spark/start-spark.sh"]