docker run --rm -d -p 8088:8088 -u 0 --network odp_intra_network --name superset superset_image

docker exec -it -u 0 superset superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password admin

docker exec -it -u 0 superset superset db upgrade
 
docker exec -it -u 0 superset superset init

docker run -d -p 7077:7077 -p 8080:8080 -p 8081:8081 -p 10000:10000 -p 10001:10001 apache/spark-py:v3.3.1

hive://hive@spark-thrift-server:10000/test_db
hive+http://hive@spark-thrift-server:10001/default
hive+http://hive@spark-thrift-server:10001/default;transportMode=http;httpPath=cliservice

jdbc:hive2://localhost:10000/default
jdbc:hive2://localhost:10001/default;transportMode=http;httpPath=cliservice
jdbc:hive2://spark-thrift-server:10001/default;transportMode=http;httpPath=cliservice
jdbc:hive2://172.30.0.8:10001/default;transportMode=http;httpPath=cliservice

!connect jdbc:hive2://spark-thrift-server:10001/default?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice
!connect jdbc:hive2://localhost:10001/hive;transportMode=http;httpPath=cliservice

hive+http://host.docker.internal:10015/test_db


docker run --rm -d -p 8088:8088 -u 0 --name superset_test apache/superset:master

docker exec -it -u 0 superset_test superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin

docker exec -it -u 0 superset_test superset db upgrade
 
docker exec -it -u 0 superset_test superset init