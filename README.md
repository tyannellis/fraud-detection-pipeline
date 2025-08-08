# Credit Card Fraud Detection – Streaming Data Pipeline to BigQuery

## Overview
This project simulates a real-time credit card transaction environment for fraud detection analytics.
A Python Kafka producer streams synthetic transaction data (including fraudulent cases) into Kafka, which is then consumed, validated, stored as Parquet in Google Cloud Storage, and loaded into BigQuery for further analysis.

The goal is to mimic a streaming architecture seen in fintech or fraud prevention systems.

## Architecture

### Data Flow
1. Producer (Python + Faker) generates synthetic credit card transactions with fields such as:
transaction_id, user_id,amount, merchant,transaction_timestamp , and is_fraud (boolean flag for simulated fraud cases)
2. Kafka Topic (transactions) buffers real-time events.
3. Consumer (Python) batches messages, validates schema, and writes Parquet files.
4. Google Cloud Storage (GCS) stores Parquet files for durability and scalability.
5. BigQuery Load Job ingests Parquet files into an analytics table for querying.
6. (Planned): MERGE logic to make ingestion idempotent and prevent duplicate rows.
7. (Planned): Transformations and modeling with dbt.
8.  (Planned): Orchestration with Apache Airflow or Composer

### Technologies Used
* Python (pandas, pyarrow, google-cloud-storage, confluent-kafka, Faker)
* Apache Kafka (with Zookeeper)
* Docker & Docker Compose
* Google Cloud Storage (GCS)
* Google BigQuery

### Key Features
* Real-Time Simulation: Continuous generation of realistic credit card transactions.
* Fraud Flagging: A subset of transactions are marked as fraudulent for testing detection logic.
* Schema Enforcement: Consumer validates fields and types before storage.
* Batch Writing: Messages written in Parquet format for efficient analytics.
* Cloud-Native Storage: Parquet files stored in GCS for durability and scalability.
* Analytics-Ready: Data loaded into BigQuery for querying and model prototyping.
* Future-Proof: Architecture structured for Airflow orchestration and dbt modeling.



