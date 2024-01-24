from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

spark_master = "spark://spark:7077"

with DAG(
    dag_id="ingest_static_data_and_players",
    tags=["batch", "ingest", "static", "players"],
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
) as dag:
    ingest_static = SparkSubmitOperator(
        task_id="ingest_static",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/ingest_static.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )

    ingest_players = SparkSubmitOperator(
        task_id="ingest_players",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/ingest_players.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )

    ingest_static >> ingest_players
