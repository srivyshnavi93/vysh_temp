#!/bin/bash

export USERNAME=manojmukkamala

docker-compose -f kafka_cluster/docker-compose.yml up -d

docker-compose -f kafka_clients/docker-compose.yml up -d

docker-compose -f spark_cluster/docker-compose.yml up -d

docker-compose -f spark_clients/docker-compose.yml up -d

docker-compose -f monitoring/docker-compose.yml up -d