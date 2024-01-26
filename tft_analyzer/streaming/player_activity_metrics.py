from pyspark.sql.functions import col, expr, countDistinct, window, when, max
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame, SparkSession
from typing import Union
from loguru import logger
import time

CHECKPOINT = "hdfs://namenode:9000/user/hive/delta/_checkpoints/"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("tft-analyzer-batch-static-preprocess")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0,org.postgresql:postgresql:42.6.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.HDFSLogStore",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            "jdbc:postgresql://hive-metastore-postgresql/metastore",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.postgresql.Driver",
        )
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.pyspark.python", "python3")
        .enableHiveSupport()
        .getOrCreate()
    )


def calculate_player_activity(spark, df):
    window_spec = window("timestamp", "1 day")
    metrics_df = (
        df.withColumn(
            "registered_today", expr("CASE WHEN action = 'register' THEN 1 ELSE 0 END")
        )
        .withColumn(
            "active_today", expr("CASE WHEN action = 'game-start' THEN 1 ELSE 0 END")
        )
        .groupBy(window_spec)
        .agg(
            expr("sum(registered_today)").alias("new_user_count"),
            expr("sum(active_today)").alias("active_user_count"),
        )
    ).withColumn("date", expr("date_trunc('day', window.start)"))

    return metrics_df


def read_stream(table: str, spark: SparkSession) -> DataFrame:
    logger.info("Reading from Delta stream...")
    df = spark.readStream.format("delta").table(table)
    logger.info("Finished reading from Delta stream.")
    return df


def show(df):
    query = (
        df.writeStream.format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )
    query.awaitTermination()


def write_stream(
    df: DataFrame, table: str, partition_by: Union[str, None] = None
) -> None:
    logger.info("Writing to Delta stream...")
    df.writeStream.format("delta").outputMode("complete").trigger(
        availableNow=True
    ).option("checkpointLocation", f"{CHECKPOINT}/gold-player-activity").partitionBy(
        partition_by
    ).toTable(
        table
    ).awaitTermination()

    logger.info("Finished writing to Delta stream.")


if __name__ == "__main__":
    spark = create_spark()
    df = read_stream("silver.logs", spark)
    metrics_df = calculate_player_activity(spark, df)
    write_stream(metrics_df, "gold.player_activity_metrics", "date")
