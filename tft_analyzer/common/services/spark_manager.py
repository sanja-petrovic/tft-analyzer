from pyspark.sql import SparkSession
from loguru import logger
from pyspark.sql.functions import col


class SparkManager:
    def __init__(self) -> None:
        self.spark: SparkSession = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName("tft-analyzer")
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.jars.packages",
                "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", "./spark-warehouse")
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.pyspark.python", "python3")
            .enableHiveSupport()
            .getOrCreate()
        )

    def initialize(self) -> None:
        self.create_schema("bronze")
        self.create_schema("silver")
        self.create_schema("gold")

    def create_schema(self, schema: str) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def table_exists(self, table, db) -> None:
        result = self.spark.sql(f"SHOW TABLES IN {db}")
        return bool(result.filter(col("tableName").contains(table)).collect())
