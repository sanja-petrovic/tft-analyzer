from pyspark.sql import SparkSession


class SparkManager:
    def __init__(self) -> None:
        self.spark: SparkSession = (
            SparkSession.builder.master("spark://localhost:7077")
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-avro_2.12:3.5.0",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", "./spark-warehouse")
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .enableHiveSupport()
            .getOrCreate()
        )

    def initialize(self) -> None:
        self.create_schema("bronze")
        self.create_schema("silver")
        self.create_schema("gold")

    def create_schema(self, schema: str) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def table_exists(self, table) -> None:
        return self.spark.catalog.tableExists(table)
