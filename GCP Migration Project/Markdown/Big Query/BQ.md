# BigQuery and Cloud Composer (Apache Airflow)
# Complete Unified Data Engineering Guide with Python Syntax

---

## 1. Overview

BigQuery is a serverless data warehouse used for large-scale analytics using SQL.

Cloud Composer is Google Cloud’s managed Apache Airflow service used to orchestrate workflows written in Python.

Cloud Composer controls WHEN jobs run.
BigQuery controls WHAT data processing happens.

---

## 2. Data Engineering Lifecycle

Source → Ingest → Store → Transform → Validate → Serve → Monitor

Sources: APIs, databases, event streams  
Ingestion: Python, GCS  
Storage & Transformations: BigQuery  
Orchestration: Cloud Composer  
Monitoring: Airflow UI and logs  

---

## 3. Cloud Composer Architecture

Components:
- Airflow Scheduler
- Airflow Webserver
- Workers
- DAGs folder
- Plugins folder
- GCS bucket for DAGs and logs

Composer bucket example:
gs://region-composer-environment-bucket

---

## 4. BigQuery Architecture

Project → Dataset → Table → Partition → Cluster

Projects contain datasets.
Datasets contain tables.
Tables store data.
Partitions improve cost and performance.
Clusters optimize filtered queries.

---

## 5. Dependencies

Preinstalled in Composer:
- apache-airflow
- apache-airflow-providers-google
- google-cloud-bigquery
- google-cloud-storage

Common Python libraries:
- pandas
- numpy
- pyarrow
- requests

---

## 6. IAM Permissions

Composer service account requires:
- roles/bigquery.admin
- roles/bigquery.jobUser
- roles/storage.objectAdmin
- roles/composer.worker

---

## 7. DAG Fundamentals

A DAG is a Python file defining:
- schedule
- tasks
- dependencies

DAGs are stored in:
gs://composer-bucket/dags

---

## 8. Minimal DAG Syntax (Python)

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="sample_dag",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end

---

## 9. Default Arguments and Retries

from datetime import timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

---

## 10. BigQuery Operator Syntax (Most Important)

BigQueryInsertJobOperator is used for all BigQuery jobs.

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

bq_query_task = BigQueryInsertJobOperator(
    task_id="run_bigquery_sql",
    configuration={
        "query": {
            "query": "SELECT COUNT(*) FROM `project.dataset.table`",
            "useLegacySql": False
        }
    }
)

---

## 11. Create BigQuery Table (SQL)

CREATE TABLE IF NOT EXISTS dataset.table_name (
  id INT64,
  name STRING,
  created_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY id;

---

## 12. Insert / Transform Data in BigQuery

INSERT INTO dataset.final_table
SELECT
  id,
  name,
  CURRENT_TIMESTAMP() AS processed_at
FROM dataset.raw_table;

---

## 13. Python BigQuery Client Syntax

Used for dynamic logic and validations.

from google.cloud import bigquery

client = bigquery.Client()

query = "SELECT COUNT(*) AS cnt FROM dataset.table"
result = client.query(query).result()

for row in result:
    print(row.cnt)

---

## 14. Load Data from GCS to BigQuery

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

load_task = GCSToBigQueryOperator(
    task_id="load_gcs_to_bq",
    bucket="my-bucket",
    source_objects=["data/file.csv"],
    destination_project_dataset_table="project.dataset.table",
    skip_leading_rows=1,
    write_disposition="WRITE_APPEND",
    autodetect=True
)

---

## 15. Task Dependency Mapping

Dependencies define execution order.

extract >> transform >> load

start >> bq_query_task >> end

---

## 16. Full End-to-End DAG Example

with DAG(
    dag_id="bq_etl_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    create_table = BigQueryInsertJobOperator(
        task_id="create_table",
        configuration={
            "query": {
                "query": "CREATE TABLE IF NOT EXISTS dataset.table (id INT64)",
                "useLegacySql": False
            }
        }
    )

    transform = BigQueryInsertJobOperator(
        task_id="transform_data",
        configuration={
            "query": {
                "query": "INSERT INTO dataset.final_table SELECT * FROM dataset.table",
                "useLegacySql": False
            }
        }
    )

    end = EmptyOperator(task_id="end")

    start >> create_table >> transform >> end

---

## 17. Airflow Variables Syntax

from airflow.models import Variable

project_id = Variable.get("gcp_project_id")
dataset = Variable.get("bq_dataset")

---

## 18. Logging Syntax

import logging

logging.info("Pipeline started")
logging.info("BigQuery job completed")

---

## 19. Scheduling Syntax

Common schedules:
@daily
@hourly
0 6 * * *

Disable backfill:
catchup=False

---

## 20. Error Handling Patterns

Use retries in default_args.
Use idempotent SQL.
Avoid partial writes.
Use WRITE_TRUNCATE or MERGE where applicable.

---

## 21. BigQuery Best Practices

- Always use partitioned tables
- Filter on partition column
- Avoid SELECT *
- Use MERGE for upserts
- Push transformations to SQL

---

## 22. Common Production Use Cases

- Daily batch ETL
- Event log aggregation
- Analytics tables
- Feature engineering
- Data quality checks

---

## 23. Mental Model

Cloud Composer = Orchestration (WHEN)
BigQuery = Processing (WHAT)
Python = Control Logic
SQL = Transformation Engine

---

## End of Unified Document
