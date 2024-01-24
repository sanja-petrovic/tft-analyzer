from common.services.job import Job


class Aggregation(Job):
    def run(self):
        # TRAIT METRICS
        match_traits_df = self.reader.read_delta("silver.match_traits")
        trait_metrics_df = self.transformer.calculate_trait_metrics(match_traits_df)
        self.writer.write_or_upsert(
            trait_metrics_df,
            "gold.trait_metrics",
            "new_table.trait_id == `gold.trait_metrics`.trait_id AND new_table.trait_tier == `gold.trait_metrics`.trait_tier",
        )

        # ITEM AND CHAMPION METRICS
        match_units_df = self.reader.read_delta("silver.match_units")
        item_df = self.transformer.calculate_item_metrics(match_units_df)
        self.writer.write(item_df, "gold.item_metrics")
        champion_metrics_df = self.transformer.calculate_champion_metrics(
            match_units_df
        )
        self.writer.write_or_upsert(
            champion_metrics_df,
            "gold.champion_metrics",
            "new_table.unit_id == `gold.champion_metrics`.unit_id AND new_table.unit_tier == `gold.champion_metrics`.unit_tier",
        )
        champion_item_df = self.transformer.calculate_champion_item_metrics(
            match_units_df
        )
        self.writer.write_or_upsert(
            champion_item_df,
            "gold.champion_item_metrics",
            "new_table.unit_id == `gold.champion_item_metrics`.unit_id AND new_table.item == `gold.champion_item_metrics`.item",
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
        # MATCH METRICS
        match_df = self.transformer.calculate_placement_metrics(
            self.reader.read_delta("silver.matches")
        )
        self.writer.write_or_upsert(
            match_df,
            "gold.placement_metrics",
            "new_table.placement == `gold.placement_metrics`.placement",
        )
        # COMPOSITION METRICS
        composition_df = self.transformer.calculate_compositions(match_traits_df)
        self.writer.write_or_upsert(
            composition_df,
            "gold.composition_metrics",
            "new_table.rank == `gold.composition_metrics`.rank",
        )
