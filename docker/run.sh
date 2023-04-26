#!/bin/bash

export USERNAME=manojmukkamala

docker-compose -f docker/kafka_cluster/docker-compose.yml up -d

docker-compose -f docker/kafka_clients/docker-compose.yml up -d

docker-compose -f docker/spark_cluster/docker-compose.yml up -d

docker-compose -f docker/spark_clients/docker-compose.yml up -d

docker-compose -f docker/monitoring/docker-compose.yml up -d