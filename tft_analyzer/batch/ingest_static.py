from tft_analyzer.common.services.job import Job

from pyspark.sql import DataFrame


class StaticIngestion(Job):
    def run(self):
        self.spark_manager.initialize()
        champions_df: DataFrame = self.reader.read_json("champions").select(
            "id", "name", "tier"
        )
        traits_df: DataFrame = self.reader.read_json("traits").select("id", "name")
        items_df: DataFrame = self.reader.read_json("items").select("id", "name")
        augments_df: DataFrame = self.reader.read_json("augments").select("id", "name")

        self.writer.write(champions_df, "bronze.champions")
        self.writer.write(traits_df, "bronze.traits")
        self.writer.write(items_df, "bronze.items")
        self.writer.write(augments_df, "bronze.augments")
