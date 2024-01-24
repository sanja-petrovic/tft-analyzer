from common.services.spark_manager import SparkManager
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count, when, round, lit, avg, desc, row_number


class Transformer:
    def __init__(self, spark_manager: SparkManager) -> None:
        self.spark_manager = spark_manager

    def calculate_trait_metrics(self, df):
        filtered_df = df.filter(col("trait_tier") > 0)
        total_matches_per_trait = filtered_df.groupBy("trait_id", "trait_tier").agg(
            count("match_id").alias("total_matches"),
            count(when(col("placement") <= 4, True)).alias("top_4_matches"),
            count(when(col("placement") == 1, True)).alias("top_1_matches"),
        )
        metrics_df = total_matches_per_trait.groupBy(
            "trait_id", "trait_tier", "total_matches", "top_4_matches", "top_1_matches"
        ).agg(
            (
                round(
                    col("total_matches") / df.select("match_id").count(),
                    4,
                )
                * 100
            ).alias("pick_rate"),
            (col("top_4_matches") / col("total_matches") * 100).alias("top_4_rate"),
            (col("top_1_matches") / col("total_matches") * 100).alias("top_1_rate"),
        )
        return metrics_df

    def calculate_champion_metrics(self, df):
        total_matches_per_champion = df.groupBy("unit_id", "unit_tier").agg(
            count("match_id").alias("total_matches"),
            count(when(col("placement") <= 4, True)).alias("top_4_matches"),
            count(when(col("placement") == 1, True)).alias("top_1_matches"),
        )
        metrics_df = total_matches_per_champion.groupBy(
            "unit_id", "unit_tier", "total_matches", "top_4_matches", "top_1_matches"
        ).agg(
            (
                round(
                    col("total_matches") / df.select("match_id").count(),
                    4,
                )
                * 100
            ).alias("pick_rate"),
            (col("top_4_matches") / col("total_matches") * 100).alias("top_4_rate"),
            (col("top_1_matches") / col("total_matches") * 100).alias("top_1_rate"),
        )
        return metrics_df

    def calculate_augment_metrics_including_pick(self, df):
        first_pick_df = df.select("match_id", "placement", "puuid", "augment1")
        second_pick_df = df.select("match_id", "placement", "puuid", "augment2")
        third_pick_df = df.select("match_id", "placement", "puuid", "augment3")

        first_metrics = self.calculate_augment_metrics(first_pick_df, 1)
        second_metrics = self.calculate_augment_metrics(second_pick_df, 2)
        third_metrics = self.calculate_augment_metrics(third_pick_df, 3)

        return (first_metrics, second_metrics, third_metrics)

    def calculate_augment_metrics(self, df, pick):
        total_matches_per_augment = df.groupBy(f"augment{pick}").agg(
            count("match_id").alias("total_matches"),
            count(when(col("placement") <= 4, True)).alias("top_4_matches"),
            count(when(col("placement") == 1, True)).alias("top_1_matches"),
        )
        metrics_df = (
            total_matches_per_augment.groupBy(
                f"augment{pick}", "total_matches", "top_4_matches", "top_1_matches"
            )
            .agg(
                (
                    round(
                        col("total_matches") / df.select("match_id").count(),
                        4,
                    )
                    * 100
                ).alias("pick_rate"),
                (col("top_4_matches") / col("total_matches") * 100).alias("top_4_rate"),
                (col("top_1_matches") / col("total_matches") * 100).alias("top_1_rate"),
            )
            .withColumnRenamed(f"augment{pick}", "augment_id")
            .withColumn("pick_order", lit(pick))
        )
        return metrics_df

    def calculate_item_metrics(self, df):
        total_matches_per_item = df.groupBy("item").agg(
            count("match_id").alias("total_matches"),
            count(when(col("placement") <= 4, True)).alias("top_4_matches"),
            count(when(col("placement") == 1, True)).alias("top_1_matches"),
        )
        metrics_df = total_matches_per_item.groupBy(
            "item", "total_matches", "top_4_matches", "top_1_matches"
        ).agg(
            (
                round(
                    col("total_matches") / df.select("match_id").count(),
                    4,
                )
                * 100
            ).alias("pick_rate"),
            (col("top_4_matches") / col("total_matches") * 100).alias("top_4_rate"),
            (col("top_1_matches") / col("total_matches") * 100).alias("top_1_rate"),
        )
        return metrics_df

    def calculate_champion_item_metrics(self, df):
        total_matches_per_champion_and_item = df.groupBy("unit_id", "item").agg(
            count("match_id").alias("total_matches"),
            count(when(col("placement") <= 4, True)).alias("top_4_matches"),
            count(when(col("placement") == 1, True)).alias("top_1_matches"),
        )
        metrics_df = total_matches_per_champion_and_item.groupBy(
            "unit_id", "item", "total_matches", "top_4_matches", "top_1_matches"
        ).agg(
            (
                round(
                    col("total_matches") / df.select("match_id").count(),
                    4,
                )
                * 100
            ).alias("pick_rate"),
            (col("top_4_matches") / col("total_matches") * 100).alias("top_4_rate"),
            (col("top_1_matches") / col("total_matches") * 100).alias("top_1_rate"),
        )
        metrics_df = (
            metrics_df.filter(~col("item").like("%Emblem%"))
            .filter(~col("item").like("%Ornn%"))
            .filter(~col("item").like("%Radiant%"))
        )
        windowSpec = Window.partitionBy("unit_id").orderBy(
            desc("pick_rate"), desc("top_4_rate")
        )
        ranked_metrics_df = metrics_df.withColumn("rank", row_number().over(windowSpec))
        final_df = ranked_metrics_df.filter(col("rank") <= 5).drop("rank")
        final_df.show(truncate=False, vertical=True)
        return final_df

    def calculate_player_metrics(self, df):
        return df.groupBy("tier").agg(
            avg("wins").alias("average_times_won"),
            avg("losses").alias("average_times_lost"),
            count("*").alias("count"),
        )

    def calculate_placement_metrics(self, df):
        metrics_df = df.groupBy("placement").agg(
            avg("gold_left").alias("average_gold_left"),
            avg("time_eliminated").alias("average_time_eliminated"),
            avg("total_damage_to_players").alias("average_damage_to_players"),
            avg("level").alias("average_level"),
            (count(when(col("level") <= 6, True)) / count("*") * 100).alias(
                "percentage_level_6_or_less"
            ),
            (count(when(col("level") == 7, True)) / count("*") * 100).alias(
                "percentage_level_7"
            ),
            (count(when(col("level") == 8, True)) / count("*") * 100).alias(
                "percentage_level_8"
            ),
            (count(when(col("level") == 10, True)) / count("*") * 100).alias(
                "percentage_level_9"
            ),
            (count(when(col("level") == 10, True)) / count("*") * 100).alias(
                "percentage_level_10"
            ),
        )

        return metrics_df

    def calculate_composition_metrics(self, df):
        pass
