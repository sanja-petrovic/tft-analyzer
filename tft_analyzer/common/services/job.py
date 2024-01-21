from __future__ import annotations


from common.services.reader import Reader
from common.services.spark_manager import SparkManager
from common.services.transformer import Transformer
from common.services.writer import Writer
from typing import Union


class Job:
    def __init__(
        self, input: Union[str, None] = None, output: Union[str, None] = None
    ) -> None:
        self.spark_manager = SparkManager()
        self.reader = Reader(self.spark_manager)
        self.transformer = Transformer(self.spark_manager)
        self.writer = Writer(self.spark_manager)
        self.input = input
        self.output = output

    def run(self):
        pass
