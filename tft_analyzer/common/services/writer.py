from __future__ import annotations

from delta import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame

from typing import Union
from common.services.spark_manager import SparkManager


class Writer:
    def __init__(self, spark_manager: SparkManager) -> Writer:
        self.spark_manager = spark_manager

    def write(
        self,
        df: DataFrame,
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
        self,
        new_alias: str,
        existing_alias: str,
        new_df: DataFrame,
        existing_table: DeltaTable,
        condition: str,
    ):
        logger.info(f'Started upserting to Delta table "{existing_alias}"...')
        existing_table.alias(existing_alias).merge(
            new_df.alias(new_alias),
            condition,
        ).whenNotMatchedInsertAll().whenMatchedUpdateAll().execute()
        logger.info(f'Finished upserting to Delta table "{existing_alias}".')

    def write_or_upsert(
        self,
        df: DataFrame,
        table_name: str,
        condition: str,
        partition_by: Union[str, None] = None,
    ):
        splitted = table_name.split(".")
        if self.spark_manager.table_exists(splitted[1], splitted[0]):
            existing_silver: DeltaTable = DeltaTable.convertToDelta(
                self.spark_manager.spark, table_name
            )
            self.upsert(
                "new_table",
                table_name,
                df,
                existing_silver,
                condition,
            )
        else:
            self.write(df, table_name, partition_by=partition_by)

    def write_stream(
        self,
        df: DataFrame,
        table: str,
        partition_by: Union[str, None] = None,
        mode: str = "append",
    ) -> None:
        df.writeStream.format("delta").outputMode(mode).option(
            "checkpointLocation", "/tmp/delta/web-server-logs/_checkpoints/"
        ).partitionBy(partition_by).toTable(table).awaitTermination()
