from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="connection",
    tags=["tft", "streaming", "aggregate", "server"],
    start_date=datetime(2024, 1, 24, 0, 0, 0),
    schedule_interval=None,
) as dag:
    connection = SparkSubmitOperator(
        task_id="connection",
        conn_id="tft_spark",
        application="/tft_analyzer/app/streaming/connection.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
    )
connection
