# Real-Time Stock Price ETL Pipeline

An end-to-end data engineering project that generates fake stock prices, streams them through Kafka, stores them in MongoDB, loads daily closing prices into PostgreSQL, and produces daily CSV reports. Everything runs locally in Docker and is orchestrated with Airflow.

## Architecture

```
Flask API -> Producer (PySpark) -> Kafka -> Consumer (PySpark) -> MongoDB
                                                                     |
                                              Airflow DAG (daily)    v
                                   Postgres `prices` table <- from_mongo_to_postgres
                                                |
                                                v
                                       5 CSV reports in outputs/
```

| Component | Role |
|---|---|
| Flask API (`exchange.py`) | Reads 300 companies from Postgres and simulates market data at `/api/market` |
| Producer (`stock_price_producer.py`) | PySpark streaming job that calls the API and publishes to the Kafka topic `stock-market-producer` |
| Kafka + Zookeeper | Message queue between producer and consumer |
| Consumer (`stock_price_consumer.py`) | PySpark streaming job that reads Kafka and writes documents to MongoDB (`mydb.mycollection`) |
| MongoDB | Raw price history (one snapshot of all companies per batch) |
| Airflow DAG (`stocks_dag`) | Daily: loads closing prices from MongoDB into Postgres, then builds the reports |
| PostgreSQL | Reference tables (companies, sectors, industries, exchanges) and daily OHLCV `prices` |
| Reports (`src/sql_report.py`) | Top volume, top gainers, top losers, average close by sector, total volume by exchange |

## Tech stack

Python, Flask, Apache Kafka, Apache Spark (PySpark Structured Streaming), MongoDB, PostgreSQL, Apache Airflow, Docker Compose, pgAdmin (optional), Confluent Control Center (optional).

## Project structure

```
.
├── docker-compose.yaml
├── Dockerfile.api              # Flask API image (Python only)
├── Dockerfile.airflow          # Airflow image (adds pymongo)
├── exchange.py                 # Flask API
├── stock_price_producer.py     # Spark -> Kafka
├── stock_price_consumer.py     # Kafka -> MongoDB
├── sql_tables.sql              # Table definitions
├── insert_fake_stock_data.sql  # 300 fake companies and lookup data
├── requirements.txt
├── airflow/dags/dag.py         # stocks_dag
├── src/
│   ├── from_mongo_to_postgres.py
│   └── sql_report.py
└── outputs/                    # Generated CSV reports
```

## Prerequisites

- Docker Desktop (about 8 GB of free RAM)
- Git
- Windows PowerShell commands are shown below; on Mac or Linux use the same `docker compose` commands

## Setup and run

All commands run from the project root.

### 1. Start the core services

```powershell
docker compose up -d postgres zookeeper broker mongodb flask-api
docker compose ps
```

Wait until `zookeeper` and `broker` show `healthy`.

### 2. Create the tables and load the data

```powershell
Get-Content .\sql_tables.sql -Raw | docker compose exec -T postgres psql -U airflow -d airflow
Get-Content .\insert_fake_stock_data.sql -Raw | docker compose exec -T postgres psql -U airflow -d airflow
```

### 3. Test the API

```powershell
curl.exe http://localhost:5000/api/market
```

### 4. Start Spark and stream the data

```powershell
docker compose up -d spark-master spark-worker
docker compose cp .\stock_price_producer.py spark-master:/tmp/
docker compose cp .\stock_price_consumer.py spark-master:/tmp/
docker compose exec -u root spark-master pip install requests pymongo psycopg2-binary "numpy<2" pandas==2.0.3
```

Producer (leave it running for a minute, then stop with `Ctrl+C`):

```powershell
docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit --master "local[2]" --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /tmp/stock_price_producer.py
```

Consumer (in a second terminal):

```powershell
docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit --master "local[2]" --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /tmp/stock_price_consumer.py
```

Check MongoDB:

```powershell
docker compose exec mongodb mongosh -u root -p example --authenticationDatabase admin --eval "db.getSiblingDB('mydb').mycollection.countDocuments()"
```

### 5. Create closing-time documents (for testing)

The DAG only writes a `prices` row for documents stamped exactly `18:00:00`. For testing, copy the latest 300 documents with a closing timestamp:

```powershell
docker compose exec mongodb mongosh -u root -p example --authenticationDatabase admin --eval "const c=db.getSiblingDB('mydb').mycollection; const last=c.find().sort({_id:-1}).limit(300).toArray(); last.forEach(d=>{delete d._id; d.timestamp='2026-10-22 18:00:00';}); c.insertMany(last); print(last.length)"
```

Use a date that matches the day before you run the reports (the reports use yesterday's date).

### 6. Start Airflow and run the DAG

```powershell
docker compose up -d --build airflow-webserver airflow-scheduler
docker compose exec airflow-webserver airflow dags unpause stocks_dag
docker compose exec airflow-webserver airflow dags trigger stocks_dag
```

Open http://localhost:8080 (login `airflow` / `airflow`) and watch the four tasks turn green. Then check the results:

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM prices;"
dir .\outputs
```

### Optional tools

| Tool | URL | Start with |
|---|---|---|
| Airflow | http://localhost:8080 | started above |
| pgAdmin | http://localhost:5050 (`admin@admin.com` / `admin`) | `docker compose up -d pgadmin` |
| Control Center | http://localhost:9021 | `docker compose up -d schema-registry control-center` |

In pgAdmin, register the server with host `postgres`, port `5432`, database `airflow`, user `airflow`, password `airflow`.

### Stop everything

```powershell
docker compose down
```

Your data is kept. Add `-v` only if you want to delete the database volume too.

## Changes from the original project

This build was based on an existing open-source project (see Acknowledgements). To get it working on a current setup, these changes were needed:

- **Flask API image:** added `Dockerfile.api` (Python only), because the original Java-based image failed on a dead PostgreSQL apt repository, and added `flask-cors` to its dependencies.
- **Airflow image:** added `Dockerfile.airflow` (Airflow plus `pymongo`, no Java) for the same reason.
- **Spark image:** switched `bitnami/spark:latest` to `bitnamilegacy/spark:3.5.0`, since the original tag no longer exists.
- **Consumer:** added the missing `from pymongo import MongoClient` import.
- **DAG:** fixed the `sys.path` entry for the `src` folder and moved `catchup=False` onto the DAG so it does not backfill hundreds of runs.
- **Reports:** the report functions now return their rows, so the CSV files are no longer empty, and the report date is yesterday instead of today.
- **Airflow webserver command:** put the `airflow users create` command on one logical line so the user is created and the webserver starts.

## Notes and limitations

- The producer and consumer are started manually with `spark-submit`; Airflow does not run them.
- The market data is simulated, so prices are random.
- The daily `volume` stored in Postgres is the maximum reading for the day, not a sum.
- Credentials in `docker-compose.yaml` (`airflow/airflow`, `root/example`, pgAdmin login) are demo values for local use only. Do not reuse them anywhere real.
- Docker containers do not restart on their own after a computer restart; start the services again with `docker compose up -d`.

## Acknowledgements

Based on the open-source project [stock-prices-etl-end-to-end-project](https://github.com/yunusgrgz1/stock-prices-etl-end-to-end-project) by Yunus Gurguz and the accompanying Dev Genius article, "Building a Real-Time Stock Price Data Pipeline with Kafka, Spark, and Airflow".