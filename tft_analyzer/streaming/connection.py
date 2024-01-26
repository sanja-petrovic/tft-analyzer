from pyspark.sql.functions import (
    col,
    expr,
    approx_count_distinct,
    count,
    window,
    mean,
    when,
)
from pyspark.sql.functions import sum as _sum
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


def calculate_placement_and_transaction_connection(spark, logs_df, matches_df):
    joined_df = logs_df.join(matches_df, logs_df.puuid == matches_df.puuid, "inner")
    windowed_df = (
        joined_df.withWatermark("timestamp", "10 minutes")
        .groupBy(window("timestamp", "1 day"), "puuid")
        .agg(
            mean("placement").alias("average_placement"),
            sum(when(joined_df.action == "buy", 1).otherwise(0)).alias(
                "transaction_count"
            ),
        )
    ).withColumn("date", expr("date_trunc('day', window.start)"))

    return windowed_df


def read_stream(table: str, spark: SparkSession) -> DataFrame:
    logger.info("Reading from Delta stream...")
    df = spark.readStream.format("delta").table(table)
    logger.info("Finished reading from Delta stream.")
    return df


def write_stream(
    df: DataFrame, table: str, partition_by: Union[str, None] = None
) -> None:
    logger.info("Writing to Delta stream...")
    df.writeStream.format("delta").outputMode("complete").trigger(
        availableNow=True
    ).option("checkpointLocation", f"{CHECKPOINT}/gold-engagement").partitionBy(
        partition_by
    ).toTable(
        table
    ).awaitTermination()
    logger.info("Finished writing to Delta stream.")


def read_delta(table: str, spark) -> DataFrame:
    logger.info(f'Started reading from "{table}"...')
    df: DataFrame = spark.read.format("delta").table(table)
    logger.info(f'Finished reading from "{table}".')
    return df


def write_delta(
    df,
    table: str,
    mode: str = "append",
    partition_by: Union[str, None] = None,
) -> None:
    logger.info(f'Started writing to Delta table "{table}"...')
    df.write.format("delta").option("mergeSchema", "true").saveAsTable(
        table, partitionBy=partition_by, mode=mode
    )
    logger.info(f'Finished writing to Delta table "{table}".')


if __name__ == "__main__":
    spark = create_spark()
    df_logs = read_stream("silver.logs", spark)
    df_matches = read_delta("silver.matches", spark)
    connection_df = calculate_placement_and_transaction_connection(
        spark, df_logs, df_matches
    )
    write_stream(connection_df, "gold.player_placement_transaction_connection", "date")
