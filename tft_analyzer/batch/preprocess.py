from typing import Union
from common.services.job import Job

from pyspark.sql.functions import explode_outer, col, when, lit, count, explode

from delta.tables import DeltaTable


class Preprocessing(Job):
    def run(self):
        df = self.reader.read_delta(self.input)
        id_column = "id"
        partition_by = None

        if self.input == "bronze.augments":
            id_column = "name"

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

        if self.input == "bronze.matches":
            partition_by = "outcome"
            df = df.dropDuplicates(["match_id", "puuid"])
            augments_df = df.select(
                "match_id",
                "placement",
                "puuid",
                col("augments")[0].alias("augment1"),
                col("augments")[1].alias("augment2"),
                col("augments")[2].alias("augment3"),
            )
            df = df.drop("augments")
            units_df = (
                df.select(
                    "match_id",
                    "placement",
                    "puuid",
                    explode_outer("units").alias("unit"),
                )
                .select(
                    "match_id",
                    "placement",
                    "puuid",
                    col("unit").getField("character_id").alias("unit_id"),
                    col("unit").getField("itemNames").alias("unit_items"),
                    col("unit").getField("tier").alias("unit_tier"),
                )
                .select(
                    "match_id",
                    "placement",
                    "puuid",
                    "unit_id",
                    col("unit_items").getItem(0).alias("unit_item_1"),
                    col("unit_items").getItem(1).alias("unit_item_2"),
                    col("unit_items").getItem(2).alias("unit_item_3"),
                    "unit_tier",
                )
            )
            df = df.drop("units")
            traits_df = df.select(
                "match_id", "placement", "puuid", explode_outer("traits").alias("trait")
            ).select(
                "match_id",
                "placement",
                "puuid",
                col("trait").getField("name").alias("trait_id"),
                col("trait").getField("num_units").alias("trait_unit_count"),
                col("trait").getField("tier_current").alias("trait_tier"),
                col("trait").getField("tier_total").alias("trait_tier_max"),
            )
            df = df.drop("traits")
            df = df.drop("companion")
            self.writer.write_or_upsert(
                augments_df,
                "silver.match_augments",
                "new_table.match_id = `silver.match_augments`.match_id AND "
                "new_table.puuid = `silver.match_augments`.puuid",
            )
            self.writer.write_or_upsert(
                traits_df,
                "silver.match_traits",
                "new_table.match_id = `silver.match_traits`.match_id AND "
                "new_table.puuid = `silver.match_traits`.puuid AND "
                "new_table.trait_id = `silver.match_traits`.trait_id",
            )
            self.writer.write_or_upsert(
                units_df,
                "silver.match_units",
                "new_table.match_id = `silver.match_units`.match_id AND "
                "new_table.puuid = `silver.match_units`.puuid AND "
                "new_table.unit_id = `silver.match_units`.unit_id AND "
                "new_table.unit_tier = `silver.match_units`.unit_tier AND "
                "new_table.unit_item_1 = `silver.match_units`.unit_item_1 AND "
                "new_table.unit_item_2 = `silver.match_units`.unit_item_2 AND "
                "new_table.unit_item_3 = `silver.match_units`.unit_item_3 AND ",
            )
            self.writer.write_or_upsert(
                df,
                self.output,
                f"new_table.match_id = `{self.output}`.match_id AND "
                f"new_table.puuid = `{self.output}`.puuid",
                partition_by,
            )
            return
        df = df.dropDuplicates([id_column])
        self.writer.write_or_upsert(
            df,
            self.output,
            f"new_table.{id_column} = `{self.output}`.{id_column}",
            partition_by,
        )
