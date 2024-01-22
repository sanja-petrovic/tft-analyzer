from common.services.spark_manager import SparkManager
from pyspark.sql.functions import col, count, when, round, lit


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
        total_matches_per_champion = df.groupBy("unit_id").agg(
            count("match_id").alias("total_matches"),
            count(when(col("placement") <= 4, True)).alias("top_4_matches"),
            count(when(col("placement") == 1, True)).alias("top_1_matches"),
        )
        metrics_df = total_matches_per_champion.groupBy(
            "unit_id", "total_matches", "top_4_matches", "top_1_matches"
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
