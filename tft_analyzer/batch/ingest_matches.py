from tft_analyzer.common.services.job import Job
import requests

import json


class MatchIngestion(Job):
    def run(self):
        puuids = list(
            self.reader.read_delta("bronze.players").select("puuid").toPandas()["puuid"]
        )
        match_ids = []
        # for puuid in puuids:
        #    data = self.get_match_ids(puuid)
        #    match_ids.extend(data)
        content = self.get_match_history("EUW1_6687599991")
        print(content)
        df = self.spark_manager.spark.sparkContext.parallelize(content)
        df = self.spark_manager.spark.read.json(df)
        df.show(vertical=True, truncate=False)

    def get_match_ids(self, puuid):
        match_ids = requests.get(
            f"https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?start=0&count=20&api_key=secret"
        )

        return match_ids.json()

    def get_match_history(self, match_id):
        match_history = requests.get(
            f"https://europe.api.riotgames.com/tft/match/v1/matches/{match_id}?api_key=secret"
        )
        return match_history.json()
