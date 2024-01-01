from __future__ import annotations


import click
from batch.ingest_matches import MatchIngestion
from batch.ingest_players import PlayerIngestion
from batch.ingest_static import StaticIngestion
from common.services.reader import Reader
from common.services.spark_manager import SparkManager
from common.services.transformer import Transformer
from common.services.writer import Writer


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
    type=click.Choice(["ingest-static", "ingest", "transform", "aggregate"]),
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
def run(
    mode: str,
    job_type: str,
    input: str,
    output: str,
):
    """Run pipeline."""
    pipeline = Pipeline(mode, job_type, input, output)
    pipeline.run()


class Pipeline:
    def __init__(
        self,
        mode: str,
        job_type: str,
        input: str,
        output: str,
    ) -> None:
        self.spark_manager = SparkManager()
        self.reader = Reader(self.spark_manager)
        self.transformer = Transformer(self.spark_manager)
        self.writer = Writer()
        self.input = input
        self.output = output
        self.mode = mode
        self.job_type = job_type
        self.job = self.get_job()

    def get_job(self):
        return StaticIngestion(self.input, self.output)

    def run(self):
        if self.job:
            return self.job.run()
