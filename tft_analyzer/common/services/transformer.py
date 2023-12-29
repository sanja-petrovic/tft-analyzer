from tft_analyzer.common.services.spark_manager import SparkManager


class Transformer:
    def __init__(self, spark_manager: SparkManager) -> None:
        self.spark_manager = spark_manager
