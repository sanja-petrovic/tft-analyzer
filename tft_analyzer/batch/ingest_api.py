from common.services.job import Job
from common.services.riot_api_handler import RiotApiHandler
from typing import Union


class ApiIngestion(Job):
    def __init__(
        self,
        api_handler: RiotApiHandler,
        input: Union[str, None] = None,
        output: Union[str, None] = None,
    ) -> None:
        super().__init__(input, output)
        self.api_handler: RiotApiHandler = api_handler

    def run(self) -> None:
        pass
