FROM ubuntu/kafka:edge

RUN apt-get update && apt-get install wget nano -y

ENV KAFKA_HOME=/opt/kafka

# For Monitoring:

RUN wget -P /opt/kafka/monitoring https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar 

RUN wget -P /opt/kafka/monitoring https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml

ENV KAFKA_OPTS="$KAFKA_OPTS -javaagent:$KAFKA_HOME/monitoring/jmx_prometheus_javaagent-0.17.2.jar=7071:$KAFKA_HOME/monitoring/kafka-2_0_0.yml"

# #

ENTRYPOINT ["/usr/bin/env"]

COPY start-kafka.sh /opt/kafka/start-kafka.sh
CMD ["bash", "/opt/kafka/start-kafka.sh"]