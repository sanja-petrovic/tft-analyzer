from uuid import uuid4

from confluent_kafka import Message, Producer
from confluent_kafka.error import KafkaError
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from loguru import logger
from common.constants.constants import BROKER_URL

from common.services.schema_registry import SchemaRegistry
from stream.log_entry import LogEntry


class MessageBroker:
    def __init__(self, schema_registry: SchemaRegistry, url: str = BROKER_URL) -> None:
        self.schema_registry = schema_registry
        self.url = url
        self.avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry.client,
            schema_str=self.schema_registry.schema_str,
        )
        self.string_serializer = StringSerializer("utf_8")
        self.producer = Producer({"bootstrap.servers": self.url})

    def delivery_report(self, error: KafkaError, message: Message):
        if error is not None:
            logger.warning(f"Delivery failed for log {message.key()}: {error}")

    def produce(self, entry: LogEntry, topic: str):
        self.producer.produce(
            topic=topic,
            key=self.string_serializer(str(uuid4())),
            value=self.avro_serializer(
                entry.to_dict(), SerializationContext(topic, MessageField.VALUE)
            ),
            on_delivery=self.delivery_report,
        )

        self.producer.flush()
