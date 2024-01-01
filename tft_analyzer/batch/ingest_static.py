from common.services.job import Job

from pyspark.sql import DataFrame


class StaticIngestion(Job):
    def run(self):
        self.reader.read_delta("bronze.test").show(vertical=True)
