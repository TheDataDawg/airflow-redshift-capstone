# Airflow + Redshift Flight Fare ETL Pipeline

This project is the capstone submission for my Master of Science in Data Analytics - Data Engineering program at WGU. It demonstrates an end-to-end ETL pipeline using **Apache Airflow**, **AWS Redshift**, and **S3** to analyze U.S. airline fare data and uncover pricing trends by quarter.

## Project Overview

- **Goal:** Load airline fare data from Amazon S3 into Redshift using Airflow and compute average flight prices by route and quarter.
- **Value:** Helps identify seasonal fare trends and expensive routes to assist travelers and analysts in decision-making.
- **Scope:** 2023 Q1–Q4 flight fare CSVs (public DOT dataset).

## Tech Stack

- **Airflow** (Dockerized, CeleryExecutor)
- **AWS Redshift Serverless** (Data warehouse)
- **Amazon S3** (Raw data storage)
- **PostgreSQL** (Airflow metadata backend)
- **Python** (ETL logic & DAG)
- **Docker Compose** (Environment orchestration)

## Project Structure

```text
airflow-redshift-capstone/
│
├── dags/
│   └── redshift_etl_dag.py     # Main DAG that loads CSVs into Redshift
│
├── plugins/helpers/            # Placeholder for custom hooks/operators if needed
├── logs/                       # Airflow logs (auto-created)
├── .env                        # Environment variables (optional)
├── docker-compose.yaml         # Full multi-service Airflow setup
├── README.md                   
└── .gitignore
