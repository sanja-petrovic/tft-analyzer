from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="calculate_metrics",
    tags=["tft", "batch", "metrics"],
    start_date=datetime(2024, 1, 25, 1, 0, 0),
    schedule_interval=None,
) as dag:
    augment_metrics = SparkSubmitOperator(
        task_id="augment_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/augment_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )
    item_metrics = SparkSubmitOperator(
        task_id="item_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/item_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )
    champion_metrics = SparkSubmitOperator(
        task_id="champion_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/aggregate/champion_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )
    trait_metrics = SparkSubmitOperator(
        task_id="trait_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/trait_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0",
    )


augment_metrics >> item_metrics >> champion_metrics >> trait_metrics