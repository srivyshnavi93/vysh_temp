FROM apache/spark-py:v3.3.1

USER 0

RUN apt-get update && apt-get install -y curl git wget nano sudo

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY conf /opt/spark/conf

RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar

COPY start-spark.sh /opt/spark/start-spark.sh
CMD ["/bin/bash", "/opt/spark/start-spark.sh"]