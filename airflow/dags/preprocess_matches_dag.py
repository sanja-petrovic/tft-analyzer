from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="preprocess_matches",
    tags=["tft", "batch", "preprocess", "match"],
    start_date=datetime(2024, 1, 24, 22, 15, 0),
    schedule_interval=None,
) as dag:
    preprocess_matches = SparkSubmitOperator(
        task_id="preprocess_matches",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/preprocess/preprocess_matches.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )

preprocess_matches
