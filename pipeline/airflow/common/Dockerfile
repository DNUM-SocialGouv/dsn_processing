FROM apache/airflow:2.7.2-python3.9

USER root
ARG AIRFLOW_UID
ARG CHAMPOLLION_GID
ARG HTTP_PROXY
WORKDIR /home/airflow
ADD --chmod=0777 . code/dsn_processing
ADD --chmod=0777 ./pipeline/airflow/dags /opt/airflow/dags

RUN apt update && apt install -y p7zip* && apt-get clean
RUN groupadd -g $CHAMPOLLION_GID CHAMPOLLION
RUN usermod -aG CHAMPOLLION airflow

USER airflow
RUN pip install --proxy=$HTTP_PROXY --no-cache-dir -r code/dsn_processing/requirements.txt