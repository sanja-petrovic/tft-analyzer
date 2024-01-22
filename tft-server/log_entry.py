from __future__ import annotations

import datetime
from dataclasses import dataclass


@dataclass
class LogEntry:
    puuid: str
    timestamp: datetime.datetime
    request_type: str
    endpoint_path: str
    status_code: int
    response_size: int
    response_time: int

    def __lt__(self, other: LogEntry) -> bool:
        return self.timestamp < other.timestamp

    def __str__(self) -> str:
        return (
            f"{self.puuid} {self.timestamp.isoformat()} {self.request_type} /{self.endpoint_path} "
            f"{self.status_code} {self.response_size} "
            f"{self.response_time}"
        )

    def to_dict(self) -> dict:
        dictionary = self.__dict__
        dictionary["timestamp"] = self.timestamp.isoformat()
        dictionary["endpoint_path"] = "/" + dictionary["endpoint_path"]
        return dictionary
