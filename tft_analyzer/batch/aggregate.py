from common.services.job import Job


class Aggregation(Job):
    def run(self):
        trait_metrics_df = self.transformer.calculate_trait_metrics(
            self.reader.read_delta("silver.match_traits")
        )
        match_units_df = self.reader.read_delta("silver.match_units")
        champion_metrics_df = self.transformer.calculate_champion_metrics(
            match_units_df
        )
        (
            augment1_metrics_df,
            augment2_metrics_df,
            augment3_metrics_df,
        ) = self.transformer.calculate_augment_metrics_including_pick(
            self.reader.read_delta("silver.match_augments")
        )
