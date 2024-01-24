from common.services.job import Job


class Aggregation(Job):
    def run(self):
        # TRAIT METRICS
        trait_metrics_df = self.transformer.calculate_trait_metrics(
            self.reader.read_delta("silver.match_traits")
        )
        self.writer.write_or_upsert(
            trait_metrics_df,
            "gold.trait_metrics",
            "new_table.trait_id == `gold.trait_metrics`.trait_id AND new_table.trait_tier == `gold.trait_metrics`.trait_tier",
        )

        # CHAMPION AND ITEM METRICS
        match_units_df = self.reader.read_delta("silver.match_units")
        champion_metrics_df = self.transformer.calculate_champion_metrics(
            match_units_df
        )
        self.writer.write_or_upsert(
            champion_metrics_df,
            "gold.champion_metrics",
            "new_table.unit_id == `gold.champion_metrics`.unit_id AND new_table.unit_tier == `gold.champion_metrics`.unit_tier",
        )
        champion_metrics_df.show(10, vertical=True, truncate=False)

        champion_item_df = self.transformer.calculate_champion_item_metrics(
            match_units_df
        )
        self.writer.write_or_upsert(
            champion_item_df,
            "gold.champion_item_metrics",
            "new_table.unit_id == `gold.champion_item_metrics`.unit_id AND new_table.item == `gold.champion_item_metrics`.item",
        )

        item_df = self.transformer.calculate_item_metrics(match_units_df)
        self.writer.write_or_upsert(
            item_df, "gold.item_metrics", "new_table.item == `gold.item_metrics`.item"
        )
        # AUGMENT METRICS
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

        # PLAYER TIER METRICS
        player_df = self.transformer.calculate_player_metrics(
            self.reader.read_delta("silver.players")
        )
        self.writer.write_or_upsert(
            player_df,
            "gold.player_metrics",
            "new_table.tier == `gold.player_metrics`.tier",
        )
        player_df.show(10, vertical=True, truncate=False)

        # MATCH METRICS
        match_df = self.transformer.calculate_placement_metrics(
            self.reader.read_delta("silver.matches")
        )
        self.writer.write_or_upsert(
            match_df,
            "gold.placement_metrics",
            "new_table.placement == `gold.placement_metrics`.placement",
        )
        match_df.show(10, vertical=True, truncate=False)
