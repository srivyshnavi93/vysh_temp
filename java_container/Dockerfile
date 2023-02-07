FROM eclipse-temurin:8u362-b09-jre-focal

USER 0


RUN mkdir /java-ex
COPY target/kafka-producer-1.0-SNAPSHOT /java-ex
RUN apt-get update && apt-get install -y curl git wget nano vim

CMD ["java", "-jar", "/kafka-producer.jar"]