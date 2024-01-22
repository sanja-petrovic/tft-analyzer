import os

SCHEMA_REGISTRY_URL: str = "http://schema-registry:8086"
BROKER_URL: str = "kafka:9092"
RIOT_API: str = os.environ["API_KEY"]
TIERS: list = ["PLATINUM", "EMERALD", "DIAMOND"]
HIGHER_TIERS: list = ["master", "grandmaster", "challenger"]
DIVISIONS: list = ["IV", "III", "II", "I"]
TOPIC: str = "tft.server.logs"
