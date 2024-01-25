FROM apache/airflow:2.6.3-python3.7

USER root

COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/lib/jvm/java-8-openjdk-amd64

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME

RUN mkdir -p /opt/spark
COPY spark-3.3.0-bin-hadoop3.tgz /opt/spark
RUN tar -xvzf /opt/spark/spark-3.3.0-bin-hadoop3.tgz --strip-components=1 -C /opt/spark
RUN rm /opt/spark/spark-3.3.0-bin-hadoop3.tgz
ENV SPARK_HOME /opt/spark
RUN export SPARK_HOME
RUN export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
RUN apt-get update \
    && apt-get -y install libpq-dev gcc
    
USER airflow

RUN pip install psycopg2

COPY requirements.txt .

RUN pip install -r requirements.txt

WORKDIR /opt/airflow
