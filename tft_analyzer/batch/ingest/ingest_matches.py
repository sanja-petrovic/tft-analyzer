from pyspark.sql import SparkSession, DataFrame
from loguru import logger
from typing import Union

import json

from pyspark.sql.functions import explode_outer, col, when, lit, current_timestamp
import time
from loguru import logger
from typing import Union
import requests

from delta import DeltaTable

RIOT_API = "SECRET"


class NotFoundError(Exception):
    """Exception raised when a resource is not found."""

    def __init__(self, message="Not found"):
        self.message = message
        super().__init__(message)


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("tft-analyzer-batch-match-ingest")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
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


def display(df: DataFrame, n: int = 1) -> None:
    df.show(n, vertical=True, truncate=False)


def read_delta(table: str, spark) -> DataFrame:
    logger.info(f'Started reading from "{table}"...')
    df: DataFrame = spark.read.format("delta").table(table)
    logger.info(f'Finished reading from "{table}".')
    return df


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


def upsert(
    new_alias: str,
    existing_alias: str,
    new_df: DataFrame,
    existing_table,
    condition: str,
):
    logger.info(f'Started upserting to Delta table "{existing_alias}"...')
    existing_table.alias(existing_alias).merge(
        new_df.alias(new_alias),
        condition,
    ).whenNotMatchedInsertAll().whenMatchedUpdateAll().execute()
    logger.info(f'Finished upserting to Delta table "{existing_alias}".')


def table_exists(spark, table, db) -> None:
    result = spark.sql(f"SHOW TABLES IN {db}")
    return bool(result.filter(col("tableName").contains(table)).collect())


def write_or_upsert(
    spark,
    df: DataFrame,
    table_name: str,
    condition: str,
    partition_by: Union[str, None] = None,
):
    splitted = table_name.split(".")
    if table_exists(spark, splitted[1], splitted[0]):
        existing_silver = DeltaTable.convertToDelta(spark, table_name)
        try:
            upsert(
                "new_table",
                table_name,
                df,
                existing_silver,
                condition,
            )
        except Exception:
            write_delta(df, table_name, partition_by=partition_by)
    else:
        write_delta(df, table_name, partition_by=partition_by)


def request_player_matches(player_id: str, count: int = 20) -> dict:
    logger.info(
        f"Started requesting player [#{player_id}]'s matches from Riot's API..."
    )
    match_ids = requests.get(
        f"https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/{player_id}/ids?start=0&count={count}&api_key={RIOT_API}",
        timeout=15,
    )
    logger.info(f"Finished requesting player [#{player_id}]'s matches from Riot's API.")

    return match_ids.json()


def request_match(match_id) -> dict:
    logger.info(
        f"Started requesting match [#{match_id}] information from Riot's API..."
    )
    match_history = requests.get(
        f"https://europe.api.riotgames.com/tft/match/v1/matches/{match_id}?api_key={RIOT_API}",
        timeout=15,
    )
    logger.info(f"Finished requesting match [#{match_id}] information from Riot's API.")
    return match_history.json()


if __name__ == "__main__":
    spark = create_spark()
    players_df = read_delta("bronze.players", spark).filter(
        col("processedTime").isNull()
    )
    puuids = list(players_df.select("puuid").toPandas()["puuid"])
    match_ids = []
    if not puuids:
        raise NotFoundError(
            "There are no unprocessed players in the database. Run the player ingestion job first."
        )

    i = 0
    for puuid in puuids:
        if i >= 5:
            break
        data = request_player_matches(puuid)
        while isinstance(data, dict) and "status" in data:
            logger.info("Sleeping for 15 seconds...")
            time.sleep(15)
            data = request_player_matches(puuid)
        i += 1
        match_ids.extend(data)
        players_df = players_df.withColumn(
            "processedTime",
            when(col("puuid") == puuid, current_timestamp()).otherwise(
                col("processedTime")
            ),
        )

    write_or_upsert(
        spark,
        players_df,
        "bronze.players",
        "new_table.idx == `bronze.players`.idx AND new_table.puuid == `bronze.players`.puuid",
    )
    match_ids = list(set(match_ids))

    for match_id in match_ids:
        match = request_match(match_id)
        while isinstance(match, dict) and "status" in match:
            logger.info("Sleeping for 15 seconds...")
            time.sleep(15)
            match = request_match(match_id)
        rdd = spark.sparkContext.parallelize([match])
        df = spark.read.json(rdd, multiLine=True)
        if "partner_group_id" in df.columns:
            continue
        df_result = (
            df.select(
                "metadata.match_id",
                "info.game_length",
                "info.tft_game_type",
                "info.tft_set_core_name",
                col("info.participants").alias("participants"),
            )
            .filter(col("tft_set_core_name") == "TFTSet10")
            .filter(col("tft_game_type") == "standard")
            .select(
                "match_id",
                "game_length",
                explode_outer("participants").alias("participant"),
            )
            .select(
                "match_id",
                "game_length",
                "participant.*",
            )
            .withColumn("outcome", when(col("placement") < 5, "win").otherwise("loss"))
        )
        try:
            write_delta(
                df_result,
                "bronze.matches",
                partition_by="outcome",
            )
        except Exception:
            continue
