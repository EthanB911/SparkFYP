# FROM openjdk:8-alpine
ARG debian_buster_image_tag=8-jre-slim
FROM openjdk:${debian_buster_image_tag}
RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*
RUN wget https://downloads.apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz
RUN tar -xzvf spark-3.0.2-bin-hadoop2.7.tgz
RUN mv spark-3.0.2-bin-hadoop2.7 /spark  
COPY start-master.sh /start-master.sh
COPY start-worker.sh /start-worker.sh
ARG shared_workspace=/opt/workspace
RUN mkdir -p ${shared_workspace} && \
    apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
ADD ./src/my_script.py .
CMD ["bash", "python", "./my_script.py"]

