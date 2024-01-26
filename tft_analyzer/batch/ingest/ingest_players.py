from loguru import logger
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, monotonically_increasing_id
import time
from pyspark.sql.types import StringType
import time
import json
from typing import Union

TIERS = ["PLATINUM", "EMERALD", "DIAMOND"]
HIGHER_TIERS = ["master", "grandmaster", "challenger"]
DIVISIONS = ["I", "II", "III", "IV"]
RIOT_API = "RGAPI-546d27a6-f1f9-495d-be79-d0f58206d9a9"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("tft-analyzer-batch-player-ingest")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.HDFSLogStore",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            "jdbc:postgresql://hive-metastore-postgresql/metastore",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.postgresql.Driver",
        )
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.pyspark.python", "python3")
        .enableHiveSupport()
        .getOrCreate()
    )


def request_players(tier: str, division: str, i: int):
    logger.info(f"Started requesting {tier} {division} players from Riot's API...")
    players = requests.get(
        f"https://euw1.api.riotgames.com/tft/league/v1/entries/{tier}/{division}?queue=RANKED_TFT&page={i}&api_key={RIOT_API}",
        timeout=15,
    )
    logger.info(f"Finished requesting {tier} {division} players from Riot's API.")
    return players.json()


def display(df: DataFrame, n: int = 1) -> None:
    df.show(n, vertical=True, truncate=False)


def write_delta(
    df,
    table: str,
    mode: str = "append",
    partition_by: Union[str, None] = None,
) -> None:
    logger.info(f'Started writing to Delta table "{table}"...')
    df.write.format("delta").option("mergeSchema", "true").saveAsTable(
        table, partitionBy=partition_by, mode=mode
    )
    logger.info(f'Finished writing to Delta table "{table}".')


def save_players(players, spark) -> None:
    rdd = spark.sparkContext.parallelize(players).map(lambda x: json.dumps(x))
    df: DataFrame = spark.read.json(rdd)
    df = df.filter(col("queueType") == "RANKED_TFT")
    df = df.withColumn("idx", monotonically_increasing_id())
    df = df.withColumn("processedTime", lit(None).cast(StringType()))
    write_delta(
        df,
        "bronze.players",
        mode="append",
    )


if __name__ == "__main__":
    spark = create_spark()
    for tier in TIERS:
        for division in DIVISIONS:
            i = 1
            while True:
                players: dict = request_players(tier, division, i)
                if not players:
                    break
                save_players(players, spark)
                if i == 3:
                    break
                i += 1
            time.sleep(10)
