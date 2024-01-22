import requests

from common.constants.constants import RIOT_API
from loguru import logger
import time


class RiotApiHandler:
    def __init__(self) -> None:
        self.base_url = "https://euw1.api.riotgames.com/tft"

    def request_players(self, tier: str, division: str, page: int) -> dict:
        logger.info(f"Started requesting {tier} {division} players from Riot's API...")
        players = requests.get(
            f"https://euw1.api.riotgames.com/tft/league/v1/entries/{tier}/{division}?queue=RANKED_TFT&page={page}&api_key={RIOT_API}",
            timeout=15,
        )
        logger.info(f"Finished requesting {tier} {division} players from Riot's API.")
        return players.json()

    def request_stronger_players(self, tier: str) -> dict:
        logger.info(f"Started requesting {tier} players from Riot's API...")
        players = requests.get(
            f"https://euw1.api.riotgames.com/tft/league/v1/{tier}?queue=RANKED_TFT&api_key={RIOT_API}",
            timeout=15,
        )
        logger.info(f"Finished requesting {tier} players from Riot's API.")
        return players.json().get("entries", None)

    def request_player_by_summoner_id(self, summoner_id: str) -> dict:
        logger.info(f"Started requesting player [#{summoner_id}] from Riot's API...")
        players = requests.get(
            f"https://euw1.api.riotgames.com/tft/league/v1/entries/by-summoner/{summoner_id}?api_key={RIOT_API}",
            timeout=15,
        )
        logger.info(f"Finished requesting player [#{summoner_id}] from Riot's API.")
        return players.json()

    def request_player_matches(self, player_id: str, count: int = 20) -> dict:
        logger.info(
            f"Started requesting player [#{player_id}]'s matches from Riot's API..."
        )
        match_ids = requests.get(
            f"https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/{player_id}/ids?start=0&count={count}&api_key={RIOT_API}",
            timeout=15,
        )
        logger.info(
            f"Finished requesting player [#{player_id}]'s matches from Riot's API."
        )

        return match_ids.json()

    def request_match(self, match_id) -> dict:
        logger.info(
            f"Started requesting match [#{match_id}] information from Riot's API..."
        )
        match_history = requests.get(
            f"https://europe.api.riotgames.com/tft/match/v1/matches/{match_id}?api_key={RIOT_API}",
            timeout=15,
        )
        logger.info(
            f"Finished requesting match [#{match_id}] information from Riot's API."
        )
        return match_history.json()
