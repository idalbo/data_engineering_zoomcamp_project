# First-time build can take upto 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# RUN apt install default-jdk
# git gcc g++ -qqq
# install here debugger for python

RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

ENV SPARK_HOME=/opt/spark
RUN export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pyspark

ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"


# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
        --bash-completion=false \
        --path-update=false \
        --usage-reporting=false \
        --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# RUN wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz \
#     && tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz \
#     && rm openjdk-11.0.2_linux-x64_bin.tar.gz

# ENV JAVA_HOME="${HOME}/spark/jdk-11.0.2"
# ENV PATH="${JAVA_HOME}/bin:${PATH}"

# RUN wget https://dlcdn.apache.org/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz \
#     && tar xzfv spark-3.0.3-bin-hadoop3.2.tgz \
#     && rm spark-3.0.3-bin-hadoop3.2.tgz

# ENV SPARK_HOME="${HOME}/spark/spark-3.0.3-bin-hadoop3.2"
# ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID
