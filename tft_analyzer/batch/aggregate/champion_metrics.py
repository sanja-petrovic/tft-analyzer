from pyspark.sql import SparkSession, DataFrame
from loguru import logger
from typing import Union

from pyspark.sql.functions import (
    col,
    count,
    when,
    round,
)
from loguru import logger
from typing import Union
from delta import DeltaTable


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("tft-analyzer-batch-trait-metrics")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
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


def upsert(
    new_alias: str,
    existing_alias: str,
    new_df: DataFrame,
    existing_table,
    condition: str,
):
    logger.info(f'Started upserting to Delta table "{existing_alias}"...')
    existing_table.alias(existing_alias).merge(
        new_df.alias(new_alias),
        condition,
    ).whenNotMatchedInsertAll().whenMatchedUpdateAll().execute()
    logger.info(f'Finished upserting to Delta table "{existing_alias}".')


def table_exists(spark, table, db) -> None:
    result = spark.sql(f"SHOW TABLES IN {db}")
    return bool(result.filter(col("tableName").contains(table)).collect())


def write_or_upsert(
    spark,
    df: DataFrame,
    table_name: str,
    condition: str,
    partition_by: Union[str, None] = None,
):
    splitted = table_name.split(".")
    if table_exists(spark, splitted[1], splitted[0]):
        try:
            existing_silver = DeltaTable.convertToDelta(spark, table_name)
            upsert(
                "new_table",
                table_name,
                df,
                existing_silver,
                condition,
            )
        except Exception:
            write_delta(df, table_name, partition_by=partition_by)
    else:
        write_delta(df, table_name, partition_by=partition_by)


def calculate_champion_metrics(df):
    """
    Questions these metrics aim to answer:
    1. How popular is each champion (pick rate)?
    2. Are players gaining rank by playing this champion (top 4 rate)?
    3. Are players winning by playing this champion (top 1 rate)?.
    """
    total_matches_per_champion = df.groupBy("unit_id", "unit_tier").agg(
        count("match_id").alias("total_matches"),
        count(when(col("placement") <= 4, True)).alias("top_4_matches"),
        count(when(col("placement") == 1, True)).alias("top_1_matches"),
    )
    metrics_df = total_matches_per_champion.groupBy(
        "unit_id", "unit_tier", "total_matches", "top_4_matches", "top_1_matches"
    ).agg(
        (
            round(
                col("total_matches") / df.select("match_id").count(),
                4,
            )
            * 100
        ).alias("pick_rate"),
        (col("top_4_matches") / col("total_matches") * 100).alias("top_4_rate"),
        (col("top_1_matches") / col("total_matches") * 100).alias("top_1_rate"),
    )
    return metrics_df


if __name__ == "__main__":
    spark = create_spark()
    match_units_df = read_delta("silver.match_units", spark)
    champion_metrics_df = calculate_champion_metrics(match_units_df)
    write_or_upsert(
        spark,
        champion_metrics_df,
        "gold.champion_metrics",
        "new_table.unit_id == `gold.champion_metrics`.unit_id AND new_table.unit_tier == `gold.champion_metrics`.unit_tier",
    )
