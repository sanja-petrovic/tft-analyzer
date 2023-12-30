from tft_analyzer.common.services.job import Job

from pyspark.sql import DataFrame


class StaticIngestion(Job):
    def run(self):
        self.spark_manager.initialize()
        self.reader.read_delta("bronze.champions").show(truncate=False, vertical=True)
