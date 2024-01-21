from common.services.job import Job
import requests
from common.exceptions.not_found import NotFoundError

import json

from pyspark.sql.functions import explode_outer, col, when, lit
from batch.ingest_api import ApiIngestion


class MatchIngestion(ApiIngestion):
    def run(self):
        puuids = list(
            self.reader.read_delta("bronze.players").select("puuid").toPandas()["puuid"]
        )
        print(len(puuids))
        match_ids = []
        if not puuids:
            raise NotFoundError(
                "There are no players in the database. Run the player ingestion job first."
            )

        for puuid in puuids:
            data = self.api_handler.request_player_matches(puuid)
            match_ids.extend(data)
            if len(match_ids) > 10:
                break

        for match_id in match_ids:
            match = self.api_handler.request_match(match_id)
            rdd = self.spark_manager.spark.sparkContext.parallelize([match])
            df = self.spark_manager.spark.read.json(rdd, multiLine=True)
            df_result = (
                df.select(
                    "metadata.match_id",
                    "info.game_length",
                    "info.tft_game_type",
                    "info.tft_set_core_name",
                    col("info.participants").alias("participants"),
                )
                .filter(col("tft_set_core_name") == "TFTSet10")
                .filter(col("tft_game_type") == "standard")
                .select(
                    "match_id",
                    "game_length",
                    explode_outer("participants").alias("participant"),
                )
                .select(
                    "match_id",
                    "game_length",
                    "participant.*",
                )
                .withColumn(
                    "outcome", when(col("placement") < 5, "win").otherwise("loss")
                )
            )
            df_result.show(vertical=True, truncate=False)
            self.writer.write(
                df_result,
                "bronze.match_history",
                mode="overwrite",
                partition_by="outcome",
            )
