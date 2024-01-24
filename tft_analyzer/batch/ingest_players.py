import json
from pyspark.sql import DataFrame

from batch.ingest_api import ApiIngestion
from common.constants.constants import TIERS, DIVISIONS, HIGHER_TIERS
from pyspark.sql.functions import lit, col, monotonically_increasing_id
from pyspark.rdd import PipelinedRDD
import time
from pyspark.sql.types import StringType


class PlayerIngestion(ApiIngestion):
    def run(self) -> None:
        for tier in TIERS:
            for division in DIVISIONS:
                i = 1
                while True:
                    players: dict = self.api_handler.request_players(tier, division, i)
                    if not players:
                        break
                    self.save_players(players)
                    if i == 3:
                        break
                    i += 1
                time.sleep(10)
        # summoner_ids = []
        # for tier in HIGHER_TIERS:
        #     stronger_players: dict = self.api_handler.request_stronger_players(tier)
        #     rdd: PipelinedRDD = self.spark_manager.spark.sparkContext.parallelize(
        #         stronger_players
        #     ).map(lambda x: json.dumps(x))
        #     data: DataFrame = (
        #         self.spark_manager.spark.read.json(rdd)
        #         .select("summonerId")
        #         .toPandas()["summonerId"]
        #     )
        #     summoner_ids.extend(data)

        # players = []
        # for summoner_id in summoner_ids:
        #     player = self.api_handler.request_player_by_summoner_id(summoner_id)
        #     players.append(player)
        #     time.sleep(1)
        # self.save_players(players)

    def save_players(self, players) -> None:
        self.spark_manager.initialize()
        rdd: PipelinedRDD = self.spark_manager.spark.sparkContext.parallelize(
            players
        ).map(lambda x: json.dumps(x))
        df: DataFrame = self.spark_manager.spark.read.json(rdd)
        df = df.filter(col("queueType") == "RANKED_TFT")
        df = df.withColumn("idx", monotonically_increasing_id())
        df = df.withColumn("processedTime", lit(None).cast(StringType()))
        self.writer.write(
            df,
            "bronze.players",
            mode="append",
        )
