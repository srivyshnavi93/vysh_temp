FROM apache/superset:master

USER root
RUN superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password admin

RUN superset db upgrade
# RUN superset superset load_examples
RUN superset init

COPY /start-superset.sh start-superset.sh
CMD [ "bash", "start-superset.sh" ]