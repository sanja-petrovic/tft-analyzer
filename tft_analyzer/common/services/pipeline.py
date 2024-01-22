from __future__ import annotations


import click
from batch.ingest_matches import MatchIngestion
from batch.preprocess import Preprocessing
from batch.ingest_players import PlayerIngestion
from batch.ingest_static import StaticIngestion
from batch.aggregate import Aggregation
from stream.ingest_logs import LogIngestion
from common.services.reader import Reader
from common.services.spark_manager import SparkManager
from common.services.transformer import Transformer
from common.services.writer import Writer
from common.services.riot_api_handler import RiotApiHandler


@click.command()
@click.option(
    "-m",
    "--mode",
    type=click.Choice(["batch", "stream"]),
    default="batch",
    show_default=True,
    help="Processing mode.",
)
@click.option(
    "-j",
    "--job_type",
    type=click.Choice(
        [
            "ingest-static",
            "ingest-players",
            "ingest-matches",
            "ingest-logs",
            "preprocess",
            "aggregate",
        ]
    ),
    help="Job type.",
)
@click.option(
    "-i",
    "--input",
    type=str,
    help="The name of the input Delta table or Kafka topic (streaming ingest).",
)
@click.option(
    "-o",
    "--output",
    type=str,
    help="The name of the output Delta table.",
)
@click.option(
    "-n",
    "--num",
    type=int,
    help="Number of matches to ingest or preprocess.",
)
def run(mode: str, job_type: str, input: str, output: str, num: int):
    """Run pipeline."""
    pipeline = Pipeline(mode, job_type, input, output, num)
    pipeline.run()


class Pipeline:
    def __init__(
        self, mode: str, job_type: str, input: str, output: str, num: int
    ) -> None:
        self.spark_manager = SparkManager()
        self.api_handler = RiotApiHandler()
        self.reader = Reader(self.spark_manager)
        self.transformer = Transformer(self.spark_manager)
        self.writer = Writer(self.spark_manager)
        self.input = input
        self.output = output
        self.mode = mode
        self.job_type = job_type
        self.num = num
        self.job = self.get_job()

    def get_job(self):
        if self.mode == "stream":
            if self.job_type == "ingest-logs":
                return LogIngestion()
        else:
            if self.job_type == "ingest-static":
                return StaticIngestion(self.input, self.output)
            elif self.job_type == "ingest-players":
                return PlayerIngestion(self.api_handler, self.input, self.output)
            elif self.job_type == "ingest-matches":
                return MatchIngestion(
                    self.api_handler, self.num, self.input, self.output
                )
            elif self.job_type == "preprocess":
                return Preprocessing(self.input, self.output)
            elif self.job_type == "aggregate":
                return Aggregation(self.input, self.output)
            else:
                return None

    def run(self):
        if self.job:
            return self.job.run()
