from __future__ import annotations

from delta import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame

from typing import Union


class Writer:
    def write(
        self,
        df: DataFrame,
        table: str,
        mode: str = "append",
        partition_by: Union[str, None] = None,
    ) -> None:
        logger.info(f'Started writing to Delta table "{table}"...')
        df.write.format("delta").option("overwriteSchema", "true").saveAsTable(
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
