FROM apache/airflow:2.5.0
USER root
RUN apt update && apt install git -y
USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt
