FROM apache/spark-py:v3.3.1

USER 0

COPY requirements.txt /
COPY conf /opt/spark/conf
COPY spark_jars /opt/spark/jars
COPY start-spark.sh /opt/spark/start-spark.sh

RUN apt-get update && apt-get install -y curl git wget nano sudo
RUN pip install --no-cache-dir -r /requirements.txt
# RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y

RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.29/mysql-connector-java-8.0.29.jar

CMD ["/bin/bash", "/opt/spark/start-spark.sh"]