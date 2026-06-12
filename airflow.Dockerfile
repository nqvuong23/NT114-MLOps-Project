ARG AIRFLOW_IMAGE_NAME
FROM ${AIRFLOW_IMAGE_NAME}

COPY airflow.requirements.txt .

RUN pip install --no-cache-dir -r airflow.requirements.txt
