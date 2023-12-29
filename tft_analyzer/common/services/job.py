from __future__ import annotations


from tft_analyzer.common.services.reader import Reader
from tft_analyzer.common.services.spark_manager import SparkManager
from tft_analyzer.common.services.transformer import Transformer
from tft_analyzer.common.services.writer import Writer


class Job:
    def __init__(self, input: str, output: str) -> None:
        self.spark_manager = SparkManager()
        self.reader = Reader(self.spark_manager)
        self.transformer = Transformer(self.spark_manager)
        self.writer = Writer()
        self.input = input
        self.output = output

    def run(self):
        pass
