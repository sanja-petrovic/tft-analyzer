from common.services.job import Job


class Aggregation(Job):
    def run(self):
        trait_metrics_df = self.transformer.calculate_trait_metrics(
            self.reader.read_delta("silver.match_traits")
        )
        self.writer.write_or_upsert(
            trait_metrics_df,
            "gold.trait_metrics",
            "new_table.trait_id == `gold.trait_metrics`.trait_id AND new_table.trait_tier == `gold.trait_metrics`.trait_tier",
        )
        match_units_df = self.reader.read_delta("silver.match_units")
        champion_metrics_df = self.transformer.calculate_champion_metrics(
            match_units_df
        )
        self.writer.write_or_upsert(
            champion_metrics_df,
            "gold.champion_metrics",
            "new_table.unit_id == `gold.champion_metrics`.unit_id",
        )
        (
            augment1_metrics_df,
            augment2_metrics_df,
            augment3_metrics_df,
        ) = self.transformer.calculate_augment_metrics_including_pick(
            self.reader.read_delta("silver.match_augments")
        )
        self.writer.write_or_upsert(
            augment1_metrics_df,
            "gold.augment_metrics",
            "new_table.augment_id == `gold.augment_metrics`.augment_id AND new_table.pick_order == `gold.augment_metrics`.pick_order",
            partition_by="pick_order",
        )
        self.writer.write_or_upsert(
            augment2_metrics_df,
            "gold.augment_metrics",
            "new_table.augment_id == `gold.augment_metrics`.augment_id AND new_table.pick_order == `gold.augment_metrics`.pick_order",
            partition_by="pick_order",
        )
        self.writer.write_or_upsert(
            augment3_metrics_df,
            "gold.augment_metrics",
            "new_table.augment_id == `gold.augment_metrics`.augment_id AND new_table.pick_order == `gold.augment_metrics`.pick_order",
            partition_by="pick_order",
        )
