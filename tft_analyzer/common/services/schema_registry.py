from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from common.constants.constants import SCHEMA_REGISTRY_URL


class SchemaRegistry:
    def __init__(self, url: str = SCHEMA_REGISTRY_URL) -> None:
        self.client = SchemaRegistryClient({"url": url})

    def get_schema_str(self, subject: str) -> str:
        return self.client.get_latest_version(subject).schema.schema_str
