from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
from tft_analyzer.common.constants.constants import BROKER_URL
from tft_analyzer.common.services.schema_registry import SchemaRegistry
from tft_analyzer.common.services.spark_manager import SparkManager


class Reader:
    def __init__(self, spark_manager: SparkManager) -> None:
        self.spark_manager = spark_manager

    def read_json(self, file: str) -> DataFrame:
        logger.info(f"Started reading {file} JSON...")
        df: DataFrame = (
            self.spark_manager.spark.read.option("multiline", "true")
            .json(f"./data/tft-{file}.json")
            .cache()
        )
        logger.info(f"Finished reading {file} JSON.")
        return df.selectExpr(f"inline({file})")

    def read_delta(self, table: str) -> DataFrame:
        logger.info(f'Started reading from "{table}"...')
        df: DataFrame = self.spark_manager.spark.read.table(table)
        logger.info(f'Finished reading from "{table}".')
        return df

    def read_topic(
        self, topic: str, alias: str, broker_url: str = BROKER_URL
    ) -> DataFrame:
        initial_df: DataFrame = (
            self.spark_manager.spark.readStream.format("kafka")
            .option("mode", "PERMISSIVE")
            .option("kafka.bootstrap.servers", broker_url)
            .option("subscribe", topic)
            .load()
        )
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

    def read_stream(self, table: str) -> DataFrame:
        return self.spark_manager.spark.readStream.format("delta").table(table)
