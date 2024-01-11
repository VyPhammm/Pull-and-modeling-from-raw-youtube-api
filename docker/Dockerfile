# select base image
FROM apache/airflow:2.5.1-python3.9
LABEL maintainer="Pham Vy (Data)"

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

EXPOSE 8080 5555 8888
