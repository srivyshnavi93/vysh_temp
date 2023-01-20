FROM apache/spark-py:v3.3.1

USER 0
RUN apt-get update && apt-get install -y curl git wget nano sudo

ARG SPARK_MASTER_WEB_UI=8080
ARG SPARK_WORKER_WEB_UI=8081
ARG SPARK_HISTORY_SERVER_WEB_UI=18080
ARG JUPYTER_LAB_WEB_UI=8888

EXPOSE ${SPARK_MASTER_WEB_UI}
EXPOSE ${SPARK_MASTER_PORT}
EXPOSE ${SPARK_WORKER_WEB_UI}
EXPOSE ${SPARK_HISTORY_SERVER_WEB_UI}
EXPOSE ${JUPYTER_LAB_WEB_UI}

RUN mkdir /opt/spark/logs \
&& touch /opt/spark/logs/spark-master.out \
&& touch /opt/spark/logs/spark-worker.out \
&& ln -sf /dev/stdout /opt/spark/logs/spark-master.out \
&& ln -sf /dev/stdout /opt/spark/logs/spark-worker.out \
&& mkdir /opt/spark/logs/hist \
&& touch /opt/spark/logs/hist/spark-logs \
&& mkdir /tmp/spark-events

COPY requirements.txt /
COPY conf /opt/spark/conf

COPY start-spark.sh /opt/spark/start-spark.sh
CMD ["/bin/bash", "/opt/spark/start-spark.sh"]