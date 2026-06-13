ARG AIRFLOW_IMAGE_NAME
FROM ${AIRFLOW_IMAGE_NAME}

USER root

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow.requirements.txt .

RUN pip install --no-cache-dir -r airflow.requirements.txt
