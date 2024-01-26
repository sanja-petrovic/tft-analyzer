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
        packages="io.delta:delta-core_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
    item_metrics = SparkSubmitOperator(
        task_id="item_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/item_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
    champion_metrics = SparkSubmitOperator(
        task_id="champion_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/champion_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
    trait_metrics = SparkSubmitOperator(
        task_id="trait_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/trait_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
    player_metrics = SparkSubmitOperator(
        task_id="player_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/player_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
    champion_item_metrics = SparkSubmitOperator(
        task_id="champion_item_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/champion_item_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
    placement_metrics = SparkSubmitOperator(
        task_id="placement_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/placement_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
    composition_metrics = SparkSubmitOperator(
        task_id="composition_metrics",
        conn_id="tft_spark",
        application="/tft_analyzer/app/batch/aggregate/composition_metrics.py",
        conf={"spark.master": "spark://spark-master:7077"},
        packages="io.delta:delta-core_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
