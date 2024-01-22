from __future__ import annotations

import datetime
import pytz
from faker import Faker
from log_entry import LogEntry
from schema_registry import SchemaRegistry
from message_broker import MessageBroker

fake = Faker()


def generate_log_entry(
    start: datetime.datetime = datetime.datetime(2021, 1, 1, 0, 0, 0),
    end: datetime.datetime = datetime.datetime(2022, 1, 1, 0, 0, 0),
    timezone: str = "Europe/Belgrade",
) -> LogEntry:
    return LogEntry(
        timestamp=fake.date_time_between(
            start_date=start,
            end_date=end,
            tzinfo=pytz.timezone(timezone),
        ),
        request_type=fake.http_method(),
        endpoint_path=fake.uri_path(),
        status_code=fake.random_element(
            elements=[
                200,
                201,
                202,
                204,
                301,
                304,
                400,
                401,
                403,
                405,
                415,
                422,
                500,
                501,
                502,
                503,
                504,
            ]
        ),
        response_size=fake.random_int(min=0, max=10_000),
        response_time=fake.random_int(min=10, max=500),
    )


if __name__ == "__main__":
    num_entries = 100
    start_datetime = datetime.datetime(2023, 10, 1, 0, 0, 0)
    end_datetime = datetime.datetime(2024, 2, 1, 0, 0, 0)
    topic = "tft.server.logs"
    print(
        f"\n- Generating {num_entries} log entries between "
        f"{start_datetime} and {end_datetime}\n"
    )

    Faker.seed(42)
    schema_registry = SchemaRegistry()
    schema_registry.register(f"{topic}-value")
    broker = MessageBroker(schema_registry)
    for _ in range(num_entries):
        broker.produce(generate_log_entry(), topic)
