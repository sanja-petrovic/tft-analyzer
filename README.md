# Teamfight Tactics Meta and Server Analyzer


## Setup
### Requirements
- Java 11
- Python 3.8
- Spark
- Docker, Compose
- Poetry

## Running instructions
### Starting Kafka
```
docker compose up
```
### Starting a job

```bash
poetry run dev run [parameters]
```

Parameters:
- `-i`, `--input`: The name of the input Delta table or Kafka topic (in case of an ingestion streaming job).
- `-o`, `--output`: The name of the output Delta table.
- `-j`, `--job`: Task to execute: `ingest-static`, `ingest`, `transform`, `aggregate`.
- `-m`, `--mode`: Processing mode (`batch` or `stream`).

## Architecture
A complete overview of the architecture can be found [here](https://s.icepanel.io/X9OaWkKYwqRoff/capb).

## Tech stack
Other than PySpark, this project primarily uses Delta Lake as a storage layer.

## Author
Sanja Petrović E2 4/2023<br>
Faculty of Technical Sciences<br>
University of Novi Sad