# BigQuery + Cloud Composer (Airflow): End‑to‑End Data Engineering Guide

This guide explains **what to install**, **how components map**, **how to code**, and **how to orchestrate pipelines** using **BigQuery + Cloud Composer** in **simple language**, covering the **full data engineering lifecycle**.

---

## 1. What Are BigQuery and Cloud Composer?

### BigQuery (BQ)

* Fully managed **serverless data warehouse**
* Used for **analytics, reporting, transformations, ML**
* Scales automatically, SQL‑based

**You use BigQuery for:**

* Storing large datasets
* Running SQL transformations
* Creating analytical tables & views

---

### Cloud Composer

* Managed **Apache Airflow** on GCP
* Used for **workflow orchestration** (scheduling + dependencies)

**You use Cloud Composer for:**

* Scheduling pipelines (daily/hourly jobs)
* Orchestrating BigQuery jobs
* Managing retries, failures, dependencies

> **Composer does NOT process data** – it *controls* tools like BigQuery.

---

## 2. High‑Level Architecture (How They Work Together)

```
Data Source
   ↓
Cloud Storage (optional)
   ↓
BigQuery (raw → staging → curated)
   ↑
Cloud Composer (Airflow DAGs)
```

**Mapping**:

* Composer triggers BigQuery jobs
* BigQuery does transformations
* Composer manages order & timing

---

## 3. Dependencies to Install (Very Important)

### 3.1 Local Development (Optional but Recommended)

```bash
pip install apache-airflow
pip install google-cloud-bigquery
pip install google-cloud-storage
pip install pandas
pip install pyarrow
```

---

### 3.2 Cloud Composer Environment (Automatic)

Cloud Composer **already includes**:

* Apache Airflow
* Google providers

But you may need to **add PyPI packages**:

```text
google-cloud-bigquery
google-cloud-storage
pandas
pyarrow
```

Add these in:

> **Composer → Environment → PyPI packages**

---

## 4. Core Components Explained (Simple Language)

### 4.1 BigQuery Components

| Component | Meaning             | Example          |
| --------- | ------------------- | ---------------- |
| Project   | Billing + container | `my-gcp-project` |
| Dataset   | Folder for tables   | `sales_ds`       |
| Table     | Actual data         | `orders`         |
| View      | Saved query         | `daily_sales_vw` |
| Partition | Split by date       | `order_date`     |
| Cluster   | Sort for speed      | `customer_id`    |

---

### 4.2 Cloud Composer Components

| Component | Meaning             |
| --------- | ------------------- |
| DAG       | Workflow definition |
| Task      | One step in DAG     |
| Operator  | Type of task        |
| Scheduler | Triggers DAG        |
| Web UI    | Monitor pipelines   |
| Workers   | Execute tasks       |

---

## 5. Data Engineering Lifecycle (BQ + Composer)

### Step 1: Ingest Data

* From APIs
* From Cloud Storage
* From databases

### Step 2: Store Raw Data

* BigQuery raw tables
* No transformations

### Step 3: Transform Data

* SQL in BigQuery
* Create staging & curated tables

### Step 4: Orchestrate

* Use Composer to schedule & control

### Step 5: Serve Data

* BI tools (Looker, Tableau, Power BI)

---

## 6. BigQuery – Common SQL Patterns

### 6.1 Create Dataset

```sql
CREATE SCHEMA IF NOT EXISTS `my_project.sales_ds`;
```

---

### 6.2 Create Table

```sql
CREATE TABLE `my_project.sales_ds.orders` (
  order_id INT64,
  customer_id INT64,
  amount FLOAT64,
  order_date DATE
);
```

---

### 6.3 Load Data from GCS

```sql
LOAD DATA INTO `my_project.sales_ds.orders`
FROM FILES (
  format = 'CSV',
  uris = ['gs://my-bucket/orders.csv']
);
```

---

### 6.4 Transform Data (ELT)

```sql
CREATE OR REPLACE TABLE `sales_ds.daily_sales` AS
SELECT
  order_date,
  SUM(amount) AS total_sales
FROM `sales_ds.orders`
GROUP BY order_date;
```

---

## 7. Python + BigQuery (Client Library)

### 7.1 Authenticate

```bash
gcloud auth application-default login
```

---

### 7.2 Query BigQuery Using Python

```python
from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT order_date, SUM(amount) AS total
FROM `sales_ds.orders`
GROUP BY order_date
"""

job = client.query(query)
df = job.to_dataframe()
print(df)
```

---

### 7.3 Insert Data Using Python

```python
rows = [
    {"order_id": 1, "customer_id": 10, "amount": 100.0, "order_date": "2025-01-01"}
]

errors = client.insert_rows_json("sales_ds.orders", rows)
print(errors)
```

---

## 8. Cloud Composer (Airflow) – Core Concepts

### 8.1 What is a DAG?

* Python file
* Defines workflow
* Stored in **GCS bucket (dags folder)**

---

### 8.2 Folder Structure

```
composer-bucket/
 └── dags/
     └── bq_pipeline.py
```

---

## 9. Airflow Operators You’ll Use Most

| Operator                           | Use Case       |
| ---------------------------------- | -------------- |
| BigQueryInsertJobOperator          | Run SQL        |
| BigQueryCreateEmptyDatasetOperator | Create dataset |
| PythonOperator                     | Custom Python  |
| DummyOperator                      | Control flow   |
| BashOperator                       | Shell commands |

---

## 10. Sample Cloud Composer DAG (Very Important)

### 10.1 Simple BigQuery Pipeline DAG

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id="bq_sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    create_daily_sales = BigQueryInsertJobOperator(
        task_id="create_daily_sales",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE sales_ds.daily_sales AS
                SELECT order_date, SUM(amount) AS total_sales
                FROM sales_ds.orders
                GROUP BY order_date
                """,
                "useLegacySql": False,
            }
        }
    )
```

---

### 10.2 Adding Task Dependencies

```python
task_1 >> task_2 >> task_3
```

---

## 11. Orchestration Patterns

### Sequential

```text
Extract → Transform → Load
```

### Parallel

```text
Transform A
Transform B
Transform C
```

### Conditional

* BranchOperator
* SensorOperator

---

## 12. Monitoring & Debugging

### Composer UI

* DAGs tab → Enable DAG
* Graph View → dependencies
* Logs → task execution

### Common Failures

* Permission errors (IAM)
* Dataset not found
* SQL syntax issues

---

## 13. IAM Permissions (Critical)

### Required Roles

| Service     | Role                 |
| ----------- | -------------------- |
| Composer SA | BigQuery Admin       |
| Composer SA | Storage Object Admin |
| User        | Composer User        |

---

## 14. Best Practices

* Use **ELT** (transform in BigQuery)
* Partition tables by date
* Avoid SELECT *
* Parameterize SQL
* Use retries in Composer

---

## 15. Typical Real‑World Use Cases

* Daily sales aggregation
* Event log processing
* Data warehouse refresh
* ML feature pipelines
* BI dashboards refresh

---

## 16. How You’ll Work Day‑to‑Day

1. Write SQL in BigQuery
2. Wrap SQL in Airflow DAG
3. Deploy DAG to Composer
4. Monitor execution
5. Optimize queries

---

## 17. Key Mental Model (Remember This)

> **Composer controls WHEN & IN WHAT ORDER**
> **BigQuery controls HOW DATA IS PROCESSED**

---

