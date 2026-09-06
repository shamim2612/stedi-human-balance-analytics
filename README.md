# STEDI Human Balance Analytics

## Project Overview

This project builds a cloud-based lakehouse solution for STEDI Step Trainer sensor data using AWS Glue, PySpark, Amazon S3, and Amazon Athena.

The objective is to process customer, accelerometer, and Step Trainer data through Landing, Trusted, and Curated zones while ensuring that only customers who agreed to share their data for research are included in the machine-learning dataset.

## Technology Stack

- AWS S3
- AWS Glue
- AWS Glue Studio
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

Privacy Requirement

Only customers who agreed to share their data for research are included in trusted and curated datasets used for machine learning.

The customer consent field is:
shareWithResearchAsOfDate

Customer records without research consent are filtered out before downstream processing.

Expected Row Counts
Zone	Table	Expected Rows
Landing	customer_landing	956
Landing	accelerometer_landing	81,273
Landing	step_trainer_landing	28,680
Trusted	customer_trusted	482
Trusted	accelerometer_trusted	40,981
Trusted	step_trainer_trusted	14,460
Curated	customer_curated	482
Curated	machine_learning_curated	43,681


Required SQL Files
sql/customer_landing.sql
sql/accelerometer_landing.sql
sql/step_trainer_landing.sql


Required Glue Jobs
glue_jobs/customer_landing_to_trusted.py
glue_jobs/accelerometer_landing_to_trusted.py
glue_jobs/customer_trusted_to_curated.py
glue_jobs/step_trainer_trusted.py
glue_jobs/machine_learning_curated.py

Validation

AWS Athena is used to validate each table after transformation.

Important checks include:

SELECT COUNT(*) FROM customer_landing;
SELECT COUNT(*) FROM accelerometer_landing;
SELECT COUNT(*) FROM step_trainer_landing;
SELECT COUNT(*) FROM customer_trusted;
SELECT COUNT(*) FROM accelerometer_trusted;
SELECT COUNT(*) FROM customer_curated;
SELECT COUNT(*) FROM step_trainer_trusted;
SELECT COUNT(*) FROM machine_learning_curated;


For the trusted customer dataset:

SELECT COUNT(*)
FROM customer_trusted
WHERE sharewithresearchasofdate IS NULL;

Expected result:

0
Project Structure
stedi-human-balance-analytics/
│
├── README.md
│
├── sql/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   └── step_trainer_landing.sql
│
├── glue_jobs/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── step_trainer_trusted.py
│   └── machine_learning_curated.py
│
└── screenshots/

Submission Checklist
 Landing SQL scripts completed
 Customer Landing to Trusted Glue job completed
 Accelerometer Landing to Trusted Glue job completed
 Customer Trusted to Curated Glue job completed
 Step Trainer Trusted Glue job completed
 Machine Learning Curated Glue job completed
 Athena row counts validated
 Required screenshots added
 All code committed to GitHub
 Final rubric review completed
