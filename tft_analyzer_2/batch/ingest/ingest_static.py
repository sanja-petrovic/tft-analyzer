from pyspark.sql import SparkSession, DataFrame
from loguru import logger
from typing import Union


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("tft-analyzer-batch-static-ingest")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.2.0",
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


def initialize(spark: SparkSession) -> None:
    create_schema("bronze", spark)
    create_schema("silver", spark)
    create_schema("gold", spark)


def create_schema(schema: str, spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def read_json(file: str, spark: SparkSession) -> DataFrame:
    logger.info(f"Started reading {file} JSON...")
    df = spark.read.option("multiline", "true").json(
        f"hdfs://namenode:9000/user/hive/data/tft-{file}.json"
    )
    logger.info(f"Finished reading {file} JSON.")
    return df.selectExpr(f"inline({file})")


def display(df: DataFrame, n: int = 1) -> None:
    df.show(n, vertical=True, truncate=False)


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
    initialize(spark)
    champions_df = read_json("champions", spark).select("id", "name", "tier")
    traits_df = read_json("traits", spark).select("id", "name")
    items_df = read_json("items", spark).select("id", "name")
    augments_df = read_json("augments", spark).select("id", "name")

    display(champions_df)

    write_delta(
        champions_df,
        "bronze.champions",
        mode="overwrite",
    )
    write_delta(
        traits_df,
        "bronze.traits",
        mode="overwrite",
    )
    write_delta(
        items_df,
        "bronze.items",
        mode="overwrite",
    )
    write_delta(
        augments_df,
        "bronze.augments",
        mode="overwrite",
    )
