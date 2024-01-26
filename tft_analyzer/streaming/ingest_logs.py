from pyspark.sql.functions import to_date, expr, col
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from typing import Union
from loguru import logger

SCHEMA_REGISTRY_URL: str = "http://schema-registry:8086"
BROKER_URL: str = "kafka:9092"
CHECKPOINT = "hdfs://namenode:9000/user/hive/delta/_checkpoints/"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("tft-analyzer-batch-trait-metrics")
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


class SchemaRegistry:
    def __init__(self, url: str = SCHEMA_REGISTRY_URL) -> None:
        self.client = SchemaRegistryClient({"url": url})

    def get_schema_str(self, subject: str) -> str:
        return self.client.get_latest_version(subject).schema.schema_str


def read_topic(
    spark, topic: str, alias: str, broker_url: str = BROKER_URL
) -> DataFrame:
    logger.info("Reading from Kafka topic...")
    initial_df: DataFrame = (
        spark.readStream.format("kafka")
        .option("mode", "PERMISSIVE")
        .option("kafka.bootstrap.servers", broker_url)
        .option("subscribe", topic)
        .load()
    )
    logger.info("Finished reading from Kafka topic, now unpacking...")
    extracted_df: DataFrame = initial_df.withColumn(
        "data", expr("substring(value, 6, length(value)-5)")
    )
    schema_registry: SchemaRegistry = SchemaRegistry()
    decoded_df: DataFrame = extracted_df.select(
        from_avro(
            col("data"),
            schema_registry.get_schema_str(f"{topic}-value"),
            {"mode": "PERMISSIVE"},
        ).alias(alias)
    )
    return decoded_df.select(f"{alias}.*")


def write_stream(
    df: DataFrame, table: str, partition_by: Union[str, None] = None
) -> None:
    logger.info("Writing to Delta stream...")
    df.writeStream.format("delta").outputMode("append").trigger(
        availableNow=True
    ).option("checkpointLocation", f"{CHECKPOINT}/bronze").partitionBy(
        partition_by
    ).toTable(
        table
    ).awaitTermination()
    logger.info("Finished writing to Delta stream.")


def show(df):
    query = df.writeStream.format("console").trigger(availableNow=True).start()
    query.awaitTermination()


if __name__ == "__main__":
    spark = create_spark()
    df = read_topic(spark, "tft.server.logs", "logs").withColumn(
        "date", to_date("timestamp")
    )

    write_stream(df, "bronze.logs", partition_by="date")
