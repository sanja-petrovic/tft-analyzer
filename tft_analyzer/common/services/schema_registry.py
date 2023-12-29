from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from tft_analyzer.common.constants.constants import SCHEMA_REGISTRY_URL


class SchemaRegistry:
    def __init__(self, url: str = SCHEMA_REGISTRY_URL) -> None:
        self.client = SchemaRegistryClient({"url": url})

    def register(self, subject: str, schema_str: str) -> int:
        schema = Schema(schema_str=schema_str, schema_type="AVRO")
        return self.client.register_schema(subject_name=subject, schema=schema)

    def get_schema_str(self, subject: str) -> str:
        return self.client.get_latest_version(subject).schema.schema_str
