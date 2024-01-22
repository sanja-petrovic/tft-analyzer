from common.services.job import Job
from pyspark.sql.functions import to_date

import time


class LogIngestion(Job):
    def run(self):
        df = self.reader.read_topic("tft.server.logs", "logs").withColumn(
            "date", to_date("timestamp")
        )

        query = (
            df.writeStream.format("console")
            .option("checkpointLocation", "/tmp/delta")
            .start()
        )

        query.awaitTermination()
