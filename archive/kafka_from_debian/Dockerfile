ARG debian_buster_image_tag=11-jre-focal
FROM eclipse-temurin:${debian_buster_image_tag}

RUN apt-get update && apt-get install vim wget nano curl -y

ARG kafka_version=3.4.0

ENV KAFKA_VERSION=$kafka_version \
    KAFKA_HOME=/opt/kafka

RUN mkdir -p ${KAFKA_HOME}   

ARG shared_workspace=/opt/kafka/work-dir
RUN mkdir -p ${shared_workspace}

ENV PATH=${PATH}:${KAFKA_HOME}/bin

RUN curl "https://downloads.apache.org/kafka/"${kafka_version}"/kafka_2.12-"${kafka_version}".tgz" -o /kafka.tgz

RUN tar -xzf /kafka.tgz --strip 1 -C $KAFKA_HOME && rm /kafka.tgz

# RUN mkdir ~/kafka && cd ~/kafka
# RUN tar -xvzf /kafka.tgz --strip 1
# RUN cd ~/kafka && mkdir data && mkdir data/kafka && mkdir data/zookeeper

# Monitoring

RUN wget -P /opt/kafka/monitoring https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar 

RUN wget -P /opt/kafka/monitoring https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml 

# Entrypoint

COPY start-kafka.sh /opt/kafka/start-kafka.sh 
CMD ["/bin/bash", "/opt/kafka/start-kafka.sh"]

# ENTRYPOINT ["/entrypoint.sh"]

# # RUN KAFKA_OPTS="$KAFKA_OPTS -javaagent:$PWD/jmx_prometheus_javaagent-0.17.2.jar=7071:$PWD/kafka-2_0_0.yml"

# CMD ["/etc/kafka/server.properties"]