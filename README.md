# STEDI Human Balance Analytics

## Project Overview

This project builds a cloud-based lakehouse solution for STEDI Step Trainer sensor data using AWS Glue, PySpark, Amazon S3, and Amazon Athena.

The objective is to process customer, accelerometer, and Step Trainer data through Landing, Trusted, and Curated zones while ensuring that only customers who agreed to share their data for research are included in the machine-learning dataset.

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
```

## Privacy Requirement

Only customers who agreed to share their data for research are included in trusted and curated datasets used for machine learning.

The customer consent field is `shareWithResearchAsOfDate`.

Customer records without research consent are filtered out before downstream processing.

## Expected Row Counts

| Zone | Table | Expected Rows |
|---|---|---:|
| Landing | `customer_landing` | 956 |
| Landing | `accelerometer_landing` | 81,273 |
| Landing | `step_trainer_landing` | 28,680 |
| Trusted | `customer_trusted` | 482 |
| Trusted | `accelerometer_trusted` | 40,981 |
| Trusted | `step_trainer_trusted` | 14,460 |
| Curated | `customer_curated` | 482 |
| Curated | `machine_learning_curated` | 43,681 |

## Required SQL Files

- `sql/customer_landing.sql`
- `sql/accelerometer_landing.sql`
- `sql/step_trainer_landing.sql`

## Required Glue Jobs

- `glue_jobs/customer_landing_to_trusted.py`
- `glue_jobs/accelerometer_landing_to_trusted.py`
- `glue_jobs/customer_trusted_to_curated.py`
- `glue_jobs/step_trainer_trusted.py`
- `glue_jobs/machine_learning_curated.py`

## Transformation Logic

### Customer Landing to Trusted
Filters customer records so that only customers who agreed to share their data for research are retained.

### Accelerometer Landing to Trusted
Inner joins `accelerometer_landing` with `customer_trusted` using `accelerometer_landing.user = customer_trusted.email`. Only accelerometer fields are retained.

### Customer Trusted to Curated
Inner joins `customer_trusted` with `accelerometer_trusted` and uses `SELECT DISTINCT` so that each eligible customer appears only once.

### Step Trainer Landing to Trusted
Inner joins `step_trainer_landing.serialNumber = customer_curated.serialnumber`.

### Machine Learning Curated
Inner joins `step_trainer_trusted.sensorReadingTime = accelerometer_trusted.timestamp`.

## Athena Validation

```sql
SELECT COUNT(*) FROM customer_landing;
SELECT COUNT(*) FROM accelerometer_landing;
SELECT COUNT(*) FROM step_trainer_landing;
SELECT COUNT(*) FROM customer_trusted;
SELECT COUNT(*) FROM accelerometer_trusted;
SELECT COUNT(*) FROM step_trainer_trusted;
SELECT COUNT(*) FROM customer_curated;
SELECT COUNT(*) FROM machine_learning_curated;
```

To verify no customer without research consent is present in the trusted zone:

```sql
SELECT COUNT(*)
FROM customer_trusted
WHERE sharewithresearchasofdate IS NULL;
```

Expected result: `0`

## Screenshots

- `customer_landing.jpg`
- `customer_landing_null_consent.jpg`
- `accelerometer_landing.jpg`
- `step_trainer_landing.jpg`
- `customer_trusted.jpg`
- `customer_trusted_no_null_consent.jpg`
- `accelerometer_trusted.jpg`
- `customer_curated.jpg`
- `step_trainer_trusted.jpg`
- `machine_learning_curated.jpg`

## Project Structure

```text
stedi-human-balance-analytics/
├── README.md
├── sql/
├── glue_jobs/
└── screenshots/
```

## Submission Checklist

- [x] Landing SQL scripts completed
- [x] Customer Landing to Trusted Glue job completed
- [x] Accelerometer Landing to Trusted Glue job completed
- [x] Customer Trusted to Curated Glue job completed
- [x] Step Trainer Trusted Glue job completed
- [x] Machine Learning Curated Glue job completed
- [x] Athena row counts validated
- [x] Required screenshots added
- [x] All code committed to GitHub
- [x] Final rubric review completed
