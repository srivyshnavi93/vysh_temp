ARG debian_buster_image_tag=11-jre-focal
FROM eclipse-temurin:${debian_buster_image_tag}

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace} && \
    apt-get update && \
    apt-get install -y curl vim nano wget software-properties-common ssh net-tools ca-certificates python3.10 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*


ENV SHARED_WORKSPACE=${shared_workspace}

VOLUME ${shared_workspace}
CMD ["bash"]