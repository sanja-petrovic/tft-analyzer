from typing import Union
from common.services.job import Job

from pyspark.sql.functions import explode_outer, col, when, lit, count

from delta.tables import DeltaTable


class Preprocessing(Job):
    def run(self):
        df = self.reader.read_delta(self.input)
        id_column = "id"
        if self.input == "bronze.augments":
            id_column = "name"
        partition_by = None
        if self.input == "bronze.players":
            id_column = "puuid"
            df = df.select(
                "puuid",
                "summonerId",
                "tier",
                "rank",
                "wins",
                "losses",
                "hotStreak",
            )
            partition_by = "tier"
            # deduplicated_df.groupBy("puuid").agg(
            #     count("puuid").alias("count_duplicates")
            # ).filter(col("count_duplicates") >= 2).show(truncate=False)

        df = df.dropDuplicates([id_column])
        if self.spark_manager.table_exists(self.output.split(".")[1], "silver"):
            existing_silver: DeltaTable = DeltaTable.convertToDelta(
                self.spark_manager.spark, self.output
            )
            self.writer.upsert(
                "new_silver",
                self.output,
                df,
                existing_silver,
                f"new_silver.{id_column} = `{self.output}`.{id_column}",
            )
        else:
            self.writer.write(df, self.output, partition_by=partition_by)

    def write_or_upsert(
        self, df, id_column: str, partition_by: Union[str, None] = None
    ):
        splitted = self.output.split(".")
        if self.spark_manager.table_exists(splitted[1], splitted[0]):
            existing_silver: DeltaTable = DeltaTable.convertToDelta(
                self.spark_manager.spark, self.output
            )
            self.writer.upsert(
                "new_silver",
                self.output,
                df,
                existing_silver,
                f"new_silver.{id_column} = `{self.output}`.{id_column}",
            )
        else:
            self.writer.write(df, self.output, partition_by=partition_by)
