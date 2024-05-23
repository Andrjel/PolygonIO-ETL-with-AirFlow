FROM apache/airflow:2.9.1

USER root

COPY requirements.txt /
COPY odbc_installation.sh /usr/src/app/

RUN chmod +x /usr/src/app/odbc_installation.sh
RUN apt-get update && apt-get install -y curl apt-transport-https

RUN /usr/src/app/odbc_installation.sh

USER airflow

RUN pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
