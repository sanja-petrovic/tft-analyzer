from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="ingest_and_preprocess_logs",
    tags=["tft", "streaming", "ingest", "preprocess", "logs"],
    start_date=datetime(2024, 1, 24, 0, 0, 0),
    schedule_interval=None,
) as dag:
    ingest_logs = SparkSubmitOperator(
        task_id="ingest_logs",
        conn_id="tft_spark",
        application="/tft_analyzer/app/streaming/ingest_logs.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
    )
    preprocess_logs = SparkSubmitOperator(
        task_id="preprocess_logs",
        conn_id="tft_spark",
        application="/tft_analyzer/app/streaming/preprocess_logs.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
    )

ingest_logs >> preprocess_logs
