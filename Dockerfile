#Dockerfile
FROM apache/airflow:2.6.0-python3.9

USER root
COPY requirements.txt ./requirements.txt
USER airflow
RUN pip install --user --upgrade pip

RUN pip install --no-cache-dir --user -r ./requirements.txt

