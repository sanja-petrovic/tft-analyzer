import requests

from tft_analyzer.common.services.job import Job

import json


class PlayerIngestion(Job):
    def run(self):
        for i in range(1, 4):
            self.get_players("DIAMOND", "I", i)
            self.get_players("DIAMOND", "II", i)
            self.get_players("DIAMOND", "III", i)
            self.get_players("DIAMOND", "IV", i)
            self.get_players("EMERALD", "I", i)
            self.get_players("EMERALD", "II", i)
            self.get_players("EMERALD", "III", i)
            self.get_players("EMERALD", "IV", i)
            self.get_players("PLATINUM", "I", i)
            self.get_players("PLATINUM", "II", i)
            self.get_players("PLATINUM", "III", i)
            self.get_players("PLATINUM", "IV", i)

    def make_request(self, tier: str, division: str, page: int):
        players = requests.get(
            f"https://euw1.api.riotgames.com/tft/league/v1/entries/{tier}/{division}?queue=RANKED_TFT&page={page}&api_key=RGAPI-546d27a6-f1f9-495d-be79-d0f58206d9a9"
        )
        return players.json()

    def get_players(self, tier: str, division: str, page: int):
        self.spark_manager.initialize()
        content = self.make_request(tier, division, page)
        df = self.spark_manager.spark.sparkContext.parallelize(content).map(
            lambda x: json.dumps(x)
        )
        df = self.spark_manager.spark.read.json(df)
        self.writer.write(df, "bronze.players", mode="overwrite")
