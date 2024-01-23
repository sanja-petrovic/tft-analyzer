from typing import Union
from common.services.job import Job
import requests
from common.exceptions.not_found import NotFoundError

import json

from pyspark.sql.functions import explode_outer, col, when, lit, current_timestamp
from batch.ingest_api import ApiIngestion
import time
from loguru import logger
from typing import Union

from common.services.riot_api_handler import RiotApiHandler


class MatchIngestion(ApiIngestion):
    def __init__(
        self,
        api_handler: RiotApiHandler,
        num: int,
        input: Union[str, None] = None,
        output: Union[str, None] = None,
    ) -> None:
        super().__init__(api_handler, input, output)
        self.num = num

    def run(self):
        players_df = self.reader.read_delta("bronze.players").filter(
            col("processedTime").isNull()
        )
        puuids = list(players_df.select("puuid").toPandas()["puuid"])
        match_ids = []
        if not puuids:
            raise NotFoundError(
                "There are no unprocessed players in the database. Run the player ingestion job first."
            )

        i = 0
        for puuid in puuids:
            if i >= self.num:
                break
            data = self.api_handler.request_player_matches(puuid)
            while isinstance(data, dict) and "status" in data:
                logger.info("Sleeping for 15 seconds...")
                time.sleep(15)
                data = self.api_handler.request_player_matches(puuid)
            i += 1
            match_ids.extend(data)
            players_df = players_df.withColumn(
                "processedTime",
                when(col("puuid") == puuid, current_timestamp()).otherwise(
                    col("processedTime")
                ),
            )

        self.writer.write_or_upsert(
            players_df, "bronze.players", "new_table.idx == `bronze.players`.idx"
        )
        match_ids = list(set(match_ids))
        with open("./match_ids.txt", "w") as f:
            for id in match_ids:
                f.write(f"{id}\n")
        for match_id in match_ids:
            match = self.api_handler.request_match(match_id)
            while isinstance(match, dict) and "status" in match:
                logger.info("Sleeping for 15 seconds...")
                time.sleep(15)
                match = self.api_handler.request_match(match_id)
            rdd = self.spark_manager.spark.sparkContext.parallelize([match])
            df = self.spark_manager.spark.read.json(rdd, multiLine=True)
            if "partner_group_id" in df.columns:
                continue
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
            try:
                self.writer.write(
                    df_result,
                    "bronze.matches",
                    partition_by="outcome",
                )
            except Exception:
                continue
