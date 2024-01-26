from pyspark.sql.functions import col, expr, rank, desc, sum
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


def calculate_transaction_metrics(spark, df):
    transaction_aggregated_df = df.groupBy(
        col("product"), expr("window(timestamp, '1 day')").alias("time_window")
    ).agg(
        expr("sum(CASE WHEN action = 'buy' THEN 1 ELSE 0 END)").alias(
            "transaction_count"
        ),
        expr("sum(CASE WHEN action = 'buy' THEN rp ELSE null END)").alias(
            "total_profit"
        ),
    )

    transaction_aggregated_df = transaction_aggregated_df.withColumn(
        "date", expr("date_trunc('day', time_window.start)")
    )

    return transaction_aggregated_df


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
    ).option("checkpointLocation", f"{CHECKPOINT}/gold-transaction").partitionBy(
        partition_by
    ).toTable(
        table
    ).awaitTermination()
    logger.info("Finished writing to Delta stream.")


if __name__ == "__main__":
    spark = create_spark()
    df = read_stream("silver.logs", spark)
    transaction_metrics = calculate_transaction_metrics(spark, df)
    write_stream(transaction_metrics, "gold.transaction_metrics", partition_by="date")
