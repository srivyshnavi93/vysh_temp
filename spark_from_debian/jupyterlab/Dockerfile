FROM spark_standalone

# -- Layer: JupyterLab

ARG spark_version=3.3.1
ARG jupyterlab_version=3.5.2

RUN apt-get update && \
    pip3 install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=''