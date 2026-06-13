ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.2.2
FROM ${AIRFLOW_IMAGE_NAME}

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY airflow.requirements.txt .

RUN pip install --no-cache-dir -r airflow.requirements.txt
