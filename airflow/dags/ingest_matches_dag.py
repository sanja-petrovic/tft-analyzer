from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="ingest_matches",
    tags=["batch", "ingest", "match"],
    start_date=datetime(2024, 1, 24, 22, 0, 0),
    schedule_interval="*/30 * * * *",
) as dag:
    ingest_matches = SparkSubmitOperator(
        task_id="ingest_matches",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/ingest_matches.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )

    ingest_matches
