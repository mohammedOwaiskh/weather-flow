# 🌦️ WeatherFlow — Real-Time Weather Streaming Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/AWS%20S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white"/>
</p>

<p align="center">
  <b>An end-to-end streaming data pipeline that ingests live weather data from IoT sensors, processes it in real time, and stores it reliably on AWS S3 — orchestrated entirely with Airflow.</b>
</p>

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
  - [Running the Pipeline](#running-the-pipeline)
- [Design Decisions](#-design-decisions)
- [Future Improvements](#-future-improvements)

---

## 🔍 Overview

**WeatherFlow** is a production-style data engineering project that simulates an IoT-based weather monitoring system. Weather stations (simulated via a data generator) continuously emit sensor readings — temperature, humidity, wind speed, pressure, and more. These readings are streamed through **Apache Kafka**, transformed using **Apache Spark**, orchestrated by **Apache Airflow**, and ultimately persisted in **AWS S3** for downstream analytics.

This project demonstrates real-world skills in **stream processing**, **distributed systems**, and **cloud data engineering** — core competencies for any modern data or AI engineering role.

---

## 🏗️ Architecture

```
┌─────────────────────────┐
│   IoT Sensor Simulator  │  ← Simulates real-time weather station data
│   (iot-data-simulator/) │
└────────────┬────────────┘
             │  Publishes JSON events
             ▼
┌─────────────────────────┐
│     Apache Kafka        │  ← Message broker for real-time data streaming
│   (Topic: weather-raw)  │     Decouples producers from consumers
└────────────┬────────────┘
             │  Consumes & transforms
             ▼
┌─────────────────────────┐
│    Apache Spark         │  ← Distributed transformation & aggregation
│   (data-processor/)     │     Cleans, enriches, and structures the stream
└────────────┬────────────┘
             │  Writes partitioned Parquet files
             ▼
┌─────────────────────────┐
│       AWS S3            │  ← Durable cloud storage for processed data
│  (s3://weather-flow/)   │     Partitioned by date/region for fast querying
└─────────────────────────┘

         ⬆ Entire workflow orchestrated by Apache Airflow DAGs ⬆
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Data Simulation** | Python | Mimics IoT weather sensor output |
| **Message Streaming** | Apache Kafka | Real-time event bus between pipeline stages |
| **Data Processing** | Apache Spark (PySpark) | Distributed transformation and enrichment |
| **Orchestration** | Apache Airflow | DAG-based scheduling and monitoring |
| **Cloud Storage** | AWS S3 | Scalable, durable sink for processed data |
| **Notebooks** | Jupyter Notebook | Exploratory analysis and development |

---

## 📁 Project Structure

```
weather-flow/
│
├── iot-data-simulator/        # IoT data generator
│   ├── simulator.py           # Produces synthetic weather readings to Kafka
│   └── config.py              # Sensor configuration (topics, frequency, fields)
│
├── data-processor/            # Spark-based stream processor
│   ├── processor.py           # Reads from Kafka, transforms, writes to S3
│   ├── transformations.py     # Data cleaning and enrichment logic
│   └── s3_writer.py           # Handles Parquet serialization and S3 upload
│
├── dags/                      # Airflow DAG definitions
│   └── weather_pipeline.py    # Main pipeline DAG: simulate → stream → store
│
├── requirements.txt           # Python dependencies
├── .gitignore
└── README.md
```

---

## 🔄 Pipeline Walkthrough

### 1. 🌡️ IoT Data Simulation (`iot-data-simulator/`)
A Python script emulates a network of distributed weather stations. Each station publishes sensor readings at a configurable interval — including fields like:
- `temperature` (°C)
- `humidity` (%)
- `wind_speed` (km/h)
- `pressure` (hPa)
- `timestamp` (ISO 8601)
- `station_id`

Events are serialized as JSON and published to a Kafka topic (`weather-raw`).

### 2. 📨 Real-Time Streaming with Apache Kafka
Kafka acts as the central nervous system of the pipeline. It decouples the data producers (IoT simulators) from consumers (Spark), enabling:
- **Fault tolerance** — messages are persisted until consumed
- **Scalability** — multiple partitions allow parallel consumption
- **Replay capability** — historical events can be re-processed if needed

### 3. ⚡ Stream Processing with Apache Spark
A PySpark Structured Streaming job consumes from the Kafka topic and applies a series of transformations:
- **Schema enforcement** — parse and validate incoming JSON
- **Data cleansing** — handle nulls, out-of-range sensor values
- **Enrichment** — derive region tags, compute moving averages
- **Aggregation** — compute windowed statistics (e.g. hourly averages per station)

Processed data is written as **Parquet** files — columnar, compressed, and optimized for analytics.

### 4. ☁️ Storage on AWS S3
Processed batches are uploaded to S3, partitioned by `date/` and `station_id/` for efficient downstream querying:

```
s3://weather-flow/
└── processed/
    ├── date=2024-10-01/
    │   ├── station_id=STN001/
    │   │   └── part-00000.parquet
    │   └── station_id=STN002/
    └── date=2024-10-02/
        └── ...
```

### 5. 🗓️ Orchestration with Apache Airflow
An Airflow DAG ties the stages together with dependency management, retry logic, and monitoring. The DAG graph looks like this:

```
start_simulator >> wait_for_data >> trigger_spark_job >> validate_output >> upload_to_s3 >> notify
```

---

## 🚀 Getting Started

### Prerequisites

Make sure the following are installed and available:

- Python 3.10+
- Java 11+ (required for Spark)
- Apache Kafka (running locally or via Docker)
- Apache Spark 3.x
- Apache Airflow 2.x
- AWS account with S3 access and configured credentials (`~/.aws/credentials`)

### Installation

```bash
# Clone the repository
git clone https://github.com/mohammedOwaiskh/weather-flow.git
cd weather-flow

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create a `.env` file at the root (see `.env.example`):

```env
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=weather-raw

# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=eu-central-1
S3_BUCKET_NAME=weather-flow

# Simulation
SENSOR_COUNT=10
EMIT_INTERVAL_SECONDS=2
```

### Running the Pipeline

**Step 1 — Start Kafka (if not already running):**
```bash
# Using Docker Compose (recommended)
docker-compose up -d zookeeper kafka
```

**Step 2 — Start the IoT Simulator:**
```bash
python iot-data-simulator/simulator.py
```

**Step 3 — Start the Spark Processor:**
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,\
             org.apache.hadoop:hadoop-aws:3.3.1 \
  data-processor/processor.py
```

**Step 4 — Trigger the Airflow DAG:**
```bash
# Initialize Airflow (first time only)
airflow db init
airflow users create --username admin --role Admin --email admin@example.com

# Start the scheduler and web server
airflow scheduler &
airflow webserver --port 8080
```

Then navigate to `http://localhost:8080`, enable and trigger the `weather_pipeline` DAG.

---

## 💡 Design Decisions

**Why Kafka instead of direct Spark ingestion?**
Kafka provides a durable buffer between data sources and consumers. If the Spark job goes down temporarily, no data is lost — it can resume from the last committed offset. This is essential in production streaming systems.

**Why Parquet on S3?**
Parquet is a columnar format that significantly reduces storage costs and speeds up analytical queries when downstream tools like AWS Athena or Spark SQL are used. Partitioning by date and station makes queries over time ranges very efficient.

**Why Airflow for orchestration?**
Airflow's DAG-based model makes the pipeline observable, retryable, and easy to extend. It provides a visual interface for monitoring pipeline health — critical for production deployments.

---

## 🔭 Future Improvements

- [ ] Add real weather API integration (e.g. OpenWeatherMap) to replace the simulator
- [ ] Build a live dashboard using Grafana or Apache Superset connected to S3/Athena
- [ ] Add data quality checks with Great Expectations
- [ ] Containerize the full pipeline with Docker Compose
- [ ] Add unit and integration tests for the transformation layer
- [ ] Deploy Kafka and Airflow to AWS (MSK + MWAA) for a fully cloud-native setup

---

## 👤 Author

**Mohammed Owais Khan**
MSc. Natural Language Processing — Universität Trier

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohammed-owais-khan/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/mohammedOwaiskh)

---

<p align="center"><i>Built to demonstrate production-grade data engineering skills — from sensor to cloud storage.</i></p>
