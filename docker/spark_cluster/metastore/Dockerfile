FROM mysql:8.3.0

ARG mysql_root_password
ARG mysql_user
ARG mysql_password
ARG mysql_database

ENV MYSQL_ROOT_PASSWORD=$mysql_root_password     
ENV MYSQL_USER=$mysql_user
ENV MYSQL_PASSWORD=$mysql_password
ENV MYSQL_DATABASE=$mysql_database

COPY hive_schema.sql /docker-entrypoint-initdb.d/

VOLUME ["/var/lib/mysql"]

# Example Build:
# docker build --build-arg mysql_root_password=metastore --build-arg mysql_user=hive_user --build-arg mysql_password=metastore --build-arg mysql_database=metastore_db . -t mysql_metastore