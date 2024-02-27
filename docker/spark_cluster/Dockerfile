FROM apache/spark-py:v3.3.1

USER 0

RUN apt-get update && apt-get install -y curl git wget nano sudo

COPY requirements.txt /

# The below command is required for the confluent-kafka requirement if Docker is running on Apple M1.
# https://github.com/confluentinc/confluent-kafka-python/issues/1326#issuecomment-1228852754
RUN apt-get update && \
  apt-get install -y --no-install-recommends gcc git libssl-dev g++ make && \
  cd /tmp && git clone https://github.com/edenhill/librdkafka.git && \
  cd librdkafka && git checkout tags/v1.9.2 && \
  ./configure && make && make install && ldconfig && \
  cd ../ && rm -rf librdkafka

RUN pip install --no-cache-dir -r /requirements.txt

COPY conf /opt/spark/conf

RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar

COPY start-spark.sh /opt/spark/start-spark.sh
CMD ["/bin/bash", "/opt/spark/start-spark.sh"]