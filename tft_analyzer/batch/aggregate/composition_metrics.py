from pyspark.sql import SparkSession, DataFrame
from loguru import logger
from typing import Union
from delta import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    count,
    when,
    avg,
    desc,
    rank,
)


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


def write_warehouse(df, collection, mode):
    df.write.format("mongodb").mode(mode).option(
        "connection.uri", "mongodb://root:123456@warehouse:27017"
    ).option("database", "gold").option("collection", collection).save()


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


def calculate_compositions(df):
    """
    Compositions are an important concept in TFT.
    They can be defined as a combination of two traits.
    Usually, players are interested in knowing the best performing compositions that will help them climb ranks.
    Questions these metrics aim to answer:
    1. How strong is each composition?
    2. How popular are these compositions? (pick rate)
    3. Are players gaining rank by playing this composition (top 1 rate)?.
    3. Are players winning by playing this composition (top 1 rate)?.
    4. What is the average placement of these compositions?
    5. What are the 25 best performing compositions?
    """
    trait_combinations = (
        df.alias("a")
        .join(
            df.alias("b"),
            (col("a.match_id") == col("b.match_id"))
            & (col("a.puuid") == col("b.puuid"))
            & (col("a.placement") == col("b.placement"))
            & (col("a.trait_id") != col("b.trait_id"))
            & (col("a.trait_tier") < col("b.trait_tier")),
            "inner",
        )
        .select(
            col("a.trait_id").alias("trait1"),
            col("b.trait_id").alias("trait2"),
            col("a.trait_tier").alias("trait1_tier"),
            col("b.trait_tier").alias("trait2_tier"),
            col("a.trait_unit_count").alias("trait1_unit_count"),
            col("b.trait_unit_count").alias("trait2_unit_count"),
            col("a.trait_tier_max").alias("trait1_tier_max"),
            col("b.trait_tier_max").alias("trait2_tier_max"),
            "a.placement",
        )
        .filter(col("trait1_tier_max") > 1)
        .filter(col("trait2_tier_max") > 1)
    )

    trait_combinations_metrics = trait_combinations.groupBy(
        "trait1",
        "trait2",
        "trait1_tier",
        "trait2_tier",
        "trait1_tier_max",
        "trait2_tier_max",
    ).agg(
        avg(col("placement")).alias("avg_placement"),
        count("*").alias("total_matches"),
        count(when(col("placement") <= 4, True)).alias("top_4_matches"),
        count(when(col("placement") == 1, True)).alias("top_1_matches"),
        (col("trait1_tier") / col("trait1_tier_max")).alias("trait1_strength"),
        (col("trait2_tier") / col("trait2_tier_max")).alias("trait2_strength"),
    )

    strength_combinations = (
        trait_combinations_metrics.withColumn(
            "pick_rate",
            (col("total_matches") / df.select("match_id").distinct().count()),
        )
        .withColumn(
            "top_4_rate",
            (col("top_4_matches") / col("total_matches") * 100).alias("top_4_rate"),
        )
        .withColumn(
            "top_1_rate",
            (col("top_1_matches") / col("total_matches") * 100).alias("top_1_rate"),
        )
        .withColumn(
            "strength",
            (8 / col("avg_placement"))
            * (col("trait1_strength") * col("trait2_strength"))
            * col("pick_rate")
            * col("top_4_rate")
            * col("top_1_rate"),
        )
    )
    average_strength = strength_combinations.groupBy(
        "trait1",
        "trait2",
    ).agg(
        avg(col("strength")).alias("strength"),
        avg(col("avg_placement")).alias("avg_placement"),
        avg(col("pick_rate")).alias("pick_rate"),
        avg(col("top_4_rate")).alias("top_4_rate"),
        avg(col("top_1_rate")).alias("top_1_rate"),
    )
    ranked_combinations = average_strength.withColumn(
        "rank", rank().over(Window.orderBy(desc("strength")))
    )
    return ranked_combinations.filter("rank <= 25")


if __name__ == "__main__":
    spark = create_spark()
    match_traits_df = read_delta("silver.match_traits", spark)
    composition_df = calculate_compositions(match_traits_df)
    write_or_upsert(
        spark,
        composition_df,
        "gold.composition_metrics",
        "new_table.rank == `gold.composition_metrics`.rank",
    )
    write_warehouse(composition_df, "composition_metrics", "append")
