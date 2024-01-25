from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

AVRO_SCHEMA = '{"type":"record","name":"logs","fields":[{"name":"puuid","type":"string","default":"NONE"},{"name":"timestamp","type":"string","default":"NONE"},{"name":"request_type","type":"string","default":"NONE"},{"name":"endpoint_path","type":"string","default":"NONE"},{"name":"status_code","type":"int","default":0},{"name":"response_size","type":"int","default":0},{"name":"response_time","type":"int","default":0}]}'


class SchemaRegistry:
    def __init__(self, url: str = "http://schema-registry:8086") -> None:
        self.client = SchemaRegistryClient({"url": url})

    def register(self, subject: str, schema_str: str = AVRO_SCHEMA) -> int:
        self.schema_str = schema_str
        schema = Schema(schema_str=schema_str, schema_type="AVRO")
        return self.client.register_schema(subject_name=subject, schema=schema)

    def get_schema_str(self, subject: str) -> str:
        return self.client.get_latest_version(subject).schema.schema_str
