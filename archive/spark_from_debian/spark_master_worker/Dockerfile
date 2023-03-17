FROM apache/spark-py

USER 0

ARG spark_version=3.3.1
ARG hadoop_version=3

# RUN apt-get update && \
#     curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
#     tar -xf spark.tgz && \
#     mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
#     mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
#     rm spark.tgz

# ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_HOME /opt/spark
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# -- Runtime

WORKDIR ${SPARK_HOME}

COPY start-spark.sh /

ARG spark_master_web_ui=8080
ARG spark_worker_web_ui=8081

EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}
EXPOSE ${spark_worker_web_ui}

CMD ["/bin/bash", "/start-spark.sh"]