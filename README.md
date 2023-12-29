# Teamfight Tactics Meta and Server Analyzer


## Setup
### Requirements
- Java 11
- Spark
- Docker, Compose

## Running instructions
### Starting Kafka
```
docker compose up
```
### Starting a job

Parameters:
- `-i`, `--input`: The name of the input Delta table or Kafka topic (in case of an ingestion streaming job).
- `-o`, `--output`: The name of the output Delta table.
- `-j`, `--job`: Task to execute: `ingest`, `transform`, `aggregate`.
- `-m`, `--mode`: Processing mode (batch or stream).

## Architecture
A complete overview of the architecture can be found [here](https://s.icepanel.io/X9OaWkKYwqRoff/capb).

## Author
Sanja Petrović E2 4/2023<br>
Faculty of Technical Sciences<br>
University of Novi Sad