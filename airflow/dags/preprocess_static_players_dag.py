from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="preprocess_static_and_players",
    tags=["tft", "batch", "preprocess", "static", "players"],
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
) as dag:
    ingest_static = SparkSubmitOperator(
        task_id="preprocess_static",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/preprocess/preprocess_static.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )

    ingest_players = SparkSubmitOperator(
        task_id="preprocess_players",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/preprocess/preprocess_players.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )

ingest_static >> ingest_players
