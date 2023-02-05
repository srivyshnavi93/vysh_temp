# Open Data Platform

- This repository holds a project built using open source tools such as Apache Kafka and Apache Spark.
- The goal of this project is to build an end to end data platform using open source tools and technologies.

### Pre-requisities:

- Docker

### Developer Details:

1. Start a docker network by running [intra_network.sh](intra_network/intra_network.sh) 
2. Create a kafka cluster by running [docker compose](kafka_cluster/docker-compose.yml) file in kafka_cluster directory.
3. Build spark image using the [Docker file](spark_cluster/Dockerfile).
4. Create a spark cluster by running [docker compose](spark_cluster/docker-compose.yml) file in spark_cluster directory.

Next Steps:

- Create Producer applicaiton
- Consumer data using Spark Structured Streaming

### Resources:
- [Docker Networking Tutorial](https://docs.docker.com/network/network-tutorial-standalone/#use-user-defined-bridge-networks)
- [apache spark docker file](https://github.com/apache/spark-docker/blob/master/3.3.1/scala2.12-java11-python3-ubuntu/Dockerfile)
- [Medium Atricle](https://towardsdatascience.com/apache-spark-cluster-on-docker-ft-a-juyterlab-interface-418383c95445)
- [Jupyter Spark image Documentation](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html#apache-spark)
- [Startup script Example](https://cloudinfrastructureservices.co.uk/create-apache-spark-docker-container-using-docker-compose/)
- [Kafka Advertised Listeners](https://rmoff.net/2018/08/02/kafka-listeners-explained/)