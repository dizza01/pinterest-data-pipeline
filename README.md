# Pinterest Data Pipeline

## Overview

This document provides a comprehensive overview of a Pinterest Data Pipeline, outlining the setup steps, components, system requirements, and the functionality of various scripts involved in both batch and stream processing. The pipeline is designed to efficiently gather, process, and send Pinterest-related data to AWS services, specifically utilizing AWS Kinesis for real-time data streaming and batch processing through Apache Spark in Databricks.

## File Stucture

pinterest-data-pipeline/
┣ 📜.gitignore
┣ 📜124f98f775af_dag.py
┣ 📜124f98f775af-key-pair.pem
┣ 📜db_creds.yaml
┣ 📜LICENSE
┣ 📜Pinterest batch processing.ipynb
┣ 📜Pinterest Kinesis Stream.ipynb
┣ 📜README.md
┣ 📜requirements.txt
┣ 📜user_post_emulation_streams.py
┗ 📜user_posting_emulation.py
┗ 📂venv/               


## System Requirements

To successfully run the Pinterest Data Pipeline, the following system requirements should be met:

- **AWS Services**:
  - **AWS EC2** for hosting the applications.
  - **AWS RDS** for the MySQL database.
  - **AWS Kinesis** for real-time data streaming.
  - **AWS MSK (Managed Streaming for Apache Kafka)** for Kafka services.
  - **S3** for data storage and processing.
  - **AWS MWAA (Managed Workflows for Apache Airflow)** for orchestrating batch workflows.

- **Software**:
  - **Java** (version 8 or higher) for running Kafka and its dependencies.
  - **Python** (version 3.6 or higher) with libraries: `requests`, `sqlalchemy`, `pymysql`, `boto3`, `yaml`, `json`, `time`, and `random`.
  - **Apache Spark** for batch data processing.
  - **Databricks** for executing the provided notebooks.
  - **Airflow** for managing the workflow and scheduling batch jobs.


## Data Processing Scripts

### 1. Batch Processing - `user_posting_emulation.py`
The `user_posting_emulation.py` script is designed for batch processing. It connects to the AWS RDS MySQL database to retrieve data and send it to specified Kafka topics. The script fetches random rows of Pinterest-related data from the database and posts this data to the appropriate Kafka topics for further processing. The batch jobs are orchestrated using AWS MWAA (Managed Workflows for Apache Airflow), with an associated DAG 124f98f775af_dag.py that schedules and manages the execution of the Pinterest batch processing.ipynb Databricks notebook.

### 2. Stream Processing - `user_post_emulation_streams.py`
The `user_post_emulation_streams.py` script is responsible for stream processing. It utilizes the Kinesis service to send data to streams in real-time. Similar to the batch processing script, it also retrieves data from the AWS RDS MySQL database but focuses on real-time data transfer to the Kinesis stream for immediate analysis and use for processing in the Pinterest Kinesis Stream.ipynb Databricks notebook.

## PySpark Processing
The data processing for batch and stream scenarios is implemented using PySpark in Databricks, as outlined in the following notebooks:

- **Pinterest Batch Processing.ipynb**: This Pyspark notebook contains the logic for processing the batched data retrieved from Kafka topics, performing transformations, and storing the User, Pin, and Geo tables in the desired format.
  
- **Pinterest Kinesis Stream.ipynb**: This Pyspark notebook focuses on processing real-time data from Kinesis streams, implementing necessary transformations to the User, Pin, and Geo tables.
