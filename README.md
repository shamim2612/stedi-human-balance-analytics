# STEDI Human Balance Analytics

## Project Overview

This project builds a cloud-based lakehouse solution for STEDI Step Trainer sensor data using AWS Glue, PySpark, Amazon S3, and Amazon Athena.

The goal is to process customer, accelerometer, and Step Trainer data through Landing, Trusted, and Curated zones while ensuring that only customers who agreed to share their data for research are included in downstream analytics and machine-learning datasets.

## Technology Stack

- AWS S3
- AWS Glue
- AWS Glue Studio
- AWS Glue Data Catalog
- AWS Athena
- Python
- PySpark / Apache Spark
- GitHub

## Data Sources

The project uses three JSON data sources:

- Customer data
- Accelerometer data
- Step Trainer data

## Lakehouse Architecture

```text
Landing Zone
│
├── customer_landing
├── accelerometer_landing
└── step_trainer_landing
        │
        ▼
Trusted Zone
│
├── customer_trusted
├── accelerometer_trusted
└── step_trainer_trusted
        │
        ▼
Curated Zone
│
├── customer_curated
└── machine_learning_curated
