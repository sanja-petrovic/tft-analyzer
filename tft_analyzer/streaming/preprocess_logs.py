from pyspark.sql.functions import to_date, expr, col, when, lit
from pyspark.sql import DataFrame, SparkSession
from typing import Union
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro
from loguru import logger
from pyspark.sql.types import StringType

CHECKPOINT = "hdfs://namenode:9000/user/hive/delta/_checkpoints/"
SCHEMA_REGISTRY_URL: str = "http://schema-registry:8086"


class SchemaRegistry:
    def __init__(self, url: str = SCHEMA_REGISTRY_URL) -> None:
        self.client = SchemaRegistryClient({"url": url})

    def get_schema_str(self, subject: str) -> str:
        return self.client.get_latest_version(subject).schema.schema_str


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("tft-analyzer-batch-static-preprocess")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
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


def read_stream(table: str, spark: SparkSession) -> DataFrame:
    logger.info("Reading from Delta stream...")
    df = spark.readStream.format("delta").table(table)
    logger.info("Finished reading from Delta stream.")
    return df


def write_stream(
    df: DataFrame, table: str, partition_by: Union[str, None] = None
) -> None:
    logger.info("Writing to Delta stream...")
    df.writeStream.format("delta").outputMode("append").trigger(
        availableNow=True
    ).option("checkpointLocation", f"{CHECKPOINT}/silver").partitionBy(
        partition_by
    ).toTable(
        table
    ).awaitTermination()
    logger.info("Finished writing to Delta stream.")


def show(df):
    query = (
        df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    spark = create_spark()
    df = read_stream("bronze.logs", spark)
    df_transformed = (
        df.withColumn(
            "product",
            when(
                col("endpoint_path").contains("buy"),
                expr(
                    "substring(endpoint_path, instr(endpoint_path, '=') + 1, instr(endpoint_path, '&') - instr(endpoint_path, '=') - 1)"
                ),
            ).otherwise(lit(None).cast(StringType())),
        )
        .withColumn(
            "rp",
            when(
                col("endpoint_path").contains("buy"),
                expr("substring(endpoint_path, -4)"),
            ).otherwise(lit(None).cast(StringType())),
        )
        .withColumn(
            "action",
            when(col("endpoint_path").contains("game"), lit("game-start"))
            .when(col("endpoint_path").contains("register"), lit("register"))
            .when(col("endpoint_path").contains("buy"), lit("transaction"))
            .otherwise(lit(None).cast(StringType())),
        )
        .drop("endpoint_path")
    )
    write_stream(df_transformed, "silver.logs", partition_by="date")
