import os

SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
BROKER_URL: str = "localhost:19092"
RIOT_API = os.environ["API_KEY"]
TIERS = ["PLATINUM", "EMERALD", "DIAMOND"]
HIGHER_TIERS = ["master", "grandmaster", "challenger"]
DIVISIONS = ["I", "II", "III", "IV"]
