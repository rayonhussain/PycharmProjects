# BigQuery → DBT → BigQuery Migration with Medallion Architecture

---

## 1. Project Overview & Architecture

### Business Context
**Scenario:** Legacy data warehouse (BigQuery) with unstructured transformations → Modern cloud-native architecture using DBT + BigQuery following medallion architecture with Cloud Composer orchestration.

**Key Goals:**
- Modernize transformation layer (manual SQL → DBT)
- Implement medallion architecture (Bronze, Silver, Gold)
- Enable version control & testing for data pipelines
- Reduce operational overhead via Cloud Composer
- Enable self-service analytics for downstream teams

---

## 2. The Full Architecture Stack

```
┌──────────────────────────────────────────────────────────────────────┐
│                    COMPLETE MIGRATION ARCHITECTURE                   │
└──────────────────────────────────────────────────────────────────────┘

LAYER 1: DATA INGESTION & ORCHESTRATION
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  Cloud Composer (Apache Airflow)                               │
│  ├── DAG: daily_ingestion_pipeline                            │
│  ├── Monitor: Data freshness & SLAs                           │
│  ├── Schedule: 2 AM daily execution                           │
│  └── Alert: Slack/Email on failures                          │
│                                                                 │
│  Legacy Sources:                                               │
│  ├── Oracle Database → GCS → BigQuery                         │
│  ├── MySQL Database → GCS → BigQuery                          │
│  ├── CSV Files → Cloud Storage → BigQuery                     │
│  └── APIs → Dataflow → BigQuery                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓

LAYER 2: BRONZE LAYER (Raw Data)
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  BigQuery Dataset: `project.bronze`                           │
│  (Exact copy of source data, no transformation)               │
│                                                                 │
│  ├── bronze.raw_customers          (from Oracle)              │
│  ├── bronze.raw_orders             (from MySQL)               │
│  ├── bronze.raw_products           (from API)                 │
│  ├── bronze.raw_transactions       (from Dataflow)            │
│  ├── bronze.raw_events             (streaming)                │
│  └── bronze.raw_metadata           (system tables)            │
│                                                                 │
│  Schema: Matches source exactly                               │
│  Retention: 2 years (cost optimization)                       │
│  Partitioning: By ingestion_date                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓

LAYER 3: DBT TRANSFORMATION LAYER
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  DBT Project: /path/to/dbt/data_warehouse                     │
│                                                                 │
│  models/bronze/ (views on raw data)                           │
│  ├── stg_customers.sql                                        │
│  ├── stg_orders.sql                                           │
│  └── stg_products.sql                                         │
│                                                                 │
│  models/silver/ (cleaned & deduplicated)                      │
│  ├── silver_customers.sql        (ephemeral/view)            │
│  ├── silver_orders.sql           (ephemeral)                 │
│  ├── silver_products.sql         (ephemeral)                 │
│  └── int_*.sql                   (intermediate logic)         │
│                                                                 │
│  models/gold/ (analytics & reporting)                         │
│  ├── fact_orders.sql             (incremental table)          │
│  ├── dim_customers.sql           (table with SCD Type 1)      │
│  ├── dim_products.sql            (table)                      │
│  └── agg_daily_sales.sql         (materialized view)          │
│                                                                 │
│  tests/ (data quality validation)                             │
│  ├── Generic tests (unique, not_null, relationships)          │
│  ├── Reconciliation tests (legacy vs new)                     │
│  └── Freshness tests (data is current)                        │
│                                                                 │
│  macros/ (reusable functions)                                 │
│  ├── generate_schema_name.sql                                 │
│  ├── dbt_hooks.sql                                            │
│  └── reconciliation_macros.sql                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓

LAYER 4: SILVER LAYER (Cleaned Data)
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  BigQuery Dataset: `project.silver`                           │
│  (Cleaned, deduplicated, validated data)                      │
│                                                                 │
│  ├── silver.customers         (ephemeral or view)             │
│  ├── silver.orders            (ephemeral)                     │
│  ├── silver.products          (ephemeral)                     │
│  ├── silver.customers_dedup   (table - SCD tracking)          │
│  └── silver_metrics.sql       (intermediate aggregations)     │
│                                                                 │
│  Operations:                                                   │
│  • Remove duplicates                                           │
│  • Handle nulls & edge cases                                   │
│  • Type conversions                                            │
│  • Regex validation                                            │
│  • Forward fill time series                                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓

LAYER 5: GOLD LAYER (Analytics Ready)
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  BigQuery Dataset: `project.gold`                             │
│  (Business-ready tables for BI/Analytics)                     │
│                                                                 │
│  FACT TABLES:                                                  │
│  ├── gold.fact_orders         (incremental, partitioned)       │
│  ├── gold.fact_transactions   (incremental)                    │
│  └── gold.fact_events         (incremental, clustered)         │
│                                                                 │
│  DIMENSION TABLES:                                             │
│  ├── gold.dim_customers       (SCD Type 1)                     │
│  ├── gold.dim_products        (SCD Type 2 via snapshots)       │
│  ├── gold.dim_dates           (time dimension)                 │
│  └── gold.dim_geography       (reference data)                 │
│                                                                 │
│  AGGREGATE TABLES:                                             │
│  ├── gold.agg_daily_sales     (materialized view)              │
│  ├── gold.agg_customer_metrics (table, refreshed hourly)       │
│  └── gold.agg_product_performance (materialized view)          │
│                                                                 │
│  SNAPSHOT TABLES (SCD Type 2):                                │
│  └── gold.dim_customers_snapshot (historical tracking)        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓

LAYER 6: ORCHESTRATION & MONITORING
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  Cloud Composer DAGs:                                          │
│                                                                 │
│  ├── dag_ingestion_bronze                                     │
│  │   (Ingest raw data to bronze layer)                       │
│  │   Tasks: GCS load → BigQuery insert → validation          │
│  │                                                             │
│  ├── dag_dbt_transform_silver                                 │
│  │   (dbt run for silver layer)                              │
│  │   Tasks: dbt parse → dbt run → dbt test                   │
│  │                                                             │
│  ├── dag_dbt_transform_gold                                   │
│  │   (dbt run for gold layer)                                │
│  │   Tasks: dbt run → dbt test → docs generate               │
│  │                                                             │
│  ├── dag_reconciliation_check                                 │
│  │   (Validate legacy vs new system)                         │
│  │   Tasks: Compare counts → Check amounts → Alert           │
│  │                                                             │
│  └── dag_snapshots_and_scd                                    │
│      (Create SCD Type 2 snapshots)                            │
│      Tasks: dbt snapshot → validate → alert                  │
│                                                                 │
│  Monitoring:                                                   │
│  • Cloud Monitoring (GCP)                                     │
│  • BigQuery audit logs                                        │
│  • dbt test results                                           │
│  • Slack/Email alerts                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓

LAYER 7: CONSUMPTION LAYER
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  BI Tools & Analytics:                                         │
│  ├── Looker/Tableau (dashboards)                              │
│  ├── Data Studio (reports)                                    │
│  ├── Python notebooks (data science)                          │
│  ├── SQL clients (ad-hoc queries)                             │
│  └── APIs (downstream applications)                           │
│                                                                 │
│  Datasets:                                                     │
│  ├── Public datasets (approved for BI)                        │
│  ├── Restricted datasets (PII handling)                       │
│  └── ML datasets (feature store)                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Why This Architecture?

### Problem: Legacy BigQuery Setup
```
Before Medallion Architecture:

Raw Data in BQ
    ↓
Manual SQL scripts in multiple locations
├── Transformation 1 (no version control)
├── Transformation 2 (no testing)
├── Transformation 3 (duplicate logic)
└── Transformation N (no documentation)
    ↓
Analytics tables (hard to maintain)
    ↓
BI Tools & Reports
    ↓
ISSUES:
❌ No version control
❌ No automated testing
❌ Duplicate transformations
❌ Hard to debug
❌ No lineage tracking
❌ Manual scheduling
❌ Difficult to audit
❌ Brittle dependencies
```

### Solution: Medallion + DBT + Composer
```
Bronze Layer (Raw):
✓ Single source of truth for raw data
✓ Easy to re-run from source
✓ Audit trail of ingestion
✓ Partition by date (cost optimization)

Silver Layer (Cleaned):
✓ Deduplicated & validated data
✓ Reusable by multiple downstream processes
✓ Data quality gates
✓ Ephemeral models save storage

Gold Layer (Analytics):
✓ Business-ready fact & dimension tables
✓ Optimized with clustering & partitioning
✓ Incremental loads (performance)
✓ Self-documenting via dbt

DBT:
✓ Version control (Git)
✓ Automated testing
✓ Lineage tracking
✓ Documentation generation
✓ Code reviews via PRs

Cloud Composer:
✓ Orchestrate multi-step pipelines
✓ Handle dependencies & retries
✓ Monitor & alert on failures
✓ Scale to hundreds of models
✓ Schedule backups & maintenance
```

---

## 4. Detailed Implementation: Bronze Layer

### Cloud Composer DAG: Bronze Ingestion

```python
# dags/dag_ingestion_bronze.py
# Ingest raw data from sources into BigQuery bronze layer

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
}

dag = DAG(
    'dag_ingestion_bronze',
    default_args=default_args,
    description='Ingest raw data from sources to bronze layer',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['bronze', 'ingestion', 'daily'],
)

# Task 1: Load Oracle customers to Bronze
load_oracle_customers = GCSToBigQueryOperator(
    task_id='load_oracle_customers_to_bronze',
    bucket='raw-data-staging',
    source_objects=['oracle_exports/customers_*.csv'],
    destination_project_dataset_table='project.bronze.raw_customers',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',  # Overwrite daily
    schema=[
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'updated_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    ],
    dag=dag
)

# Task 2: Load MySQL orders to Bronze
load_mysql_orders = GCSToBigQueryOperator(
    task_id='load_mysql_orders_to_bronze',
    bucket='raw-data-staging',
    source_objects=['mysql_exports/orders_*.csv'],
    destination_project_dataset_table='project.bronze.raw_orders',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    schema=[
        {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'order_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'amount', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=dag
)

# Task 3: Validate Bronze Data Quality
def validate_bronze_data(**context):
    """Check data quality in bronze layer"""
    from google.cloud import bigquery
    import logging
    
    client = bigquery.Client(project='project-id')
    logger = logging.getLogger(__name__)
    
    validation_query = """
    SELECT
      'customers' as table_name,
      COUNT(*) as row_count,
      COUNTIF(customer_id IS NULL) as null_ids,
      CURRENT_TIMESTAMP() as check_time
    FROM `project.bronze.raw_customers`
    UNION ALL
    SELECT
      'orders',
      COUNT(*),
      COUNTIF(order_id IS NULL),
      CURRENT_TIMESTAMP()
    FROM `project.bronze.raw_orders`
    """
    
    results = client.query(validation_query).result()
    for row in results:
        logger.info(f"Table {row.table_name}: {row.row_count} rows, {row.null_ids} nulls")
        
        # Fail if too many nulls
        if row.null_ids > (row.row_count * 0.05):
            raise ValueError(f"Data quality check failed: {row.table_name} has >5% nulls")
    
    context['task_instance'].xcom_push(
        key='validation_passed',
        value=True
    )

validate_bronze = PythonOperator(
    task_id='validate_bronze_data',
    python_callable=validate_bronze_data,
    provide_context=True,
    dag=dag
)

# Task 4: Update Bronze metadata table
update_bronze_metadata = BigQueryInsertJobOperator(
    task_id='update_bronze_metadata',
    configuration={
        'query': {
            'query': """
            INSERT INTO `project.bronze.bronze_metadata` 
            (table_name, row_count, last_loaded_at)
            SELECT
              'raw_customers' as table_name,
              COUNT(*) as row_count,
              CURRENT_TIMESTAMP() as last_loaded_at
            FROM `project.bronze.raw_customers`
            UNION ALL
            SELECT
              'raw_orders',
              COUNT(*),
              CURRENT_TIMESTAMP()
            FROM `project.bronze.raw_orders`
            """,
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag
)

# Define task dependencies
[load_oracle_customers, load_mysql_orders] >> validate_bronze >> update_bronze_metadata
```

**Why Bronze Layer?**
- Single source of truth for raw data
- Audit trail (know exactly when data was loaded)
- Easy to re-run transformations without re-extracting
- Cost optimization with partitioning & retention policies

---

## 5. Silver Layer: DBT Cleaning & Deduplication

### stg_customers.sql (Bronze → Silver)

```sql
-- models/silver/stg_customers.sql
-- Clean and deduplicate customer data from bronze

{{ config(
    materialized='ephemeral',  -- Not stored, inlined into downstream
    schema='silver',
    tags=['silver', 'customers', 'daily'],
    description='Cleaned customer staging from bronze raw data'
) }}

WITH source_data AS (
  SELECT
    customer_id,
    customer_name,
    email,
    country,
    created_date,
    updated_date
  FROM {{ source('raw', 'raw_customers') }}  -- bronze.raw_customers
  WHERE customer_id IS NOT NULL
)

SELECT
  customer_id,
  UPPER(TRIM(customer_name)) as customer_name,
  LOWER(TRIM(email)) as email,
  CASE 
    WHEN country IS NULL THEN 'Unknown'
    ELSE UPPER(TRIM(country)) 
  END as country,
  CAST(created_date AS TIMESTAMP) as created_at,
  CAST(updated_date AS TIMESTAMP) as updated_at,
  CURRENT_TIMESTAMP() as _dbt_silver_at
FROM source_data
```

### silver_customers_dedup.sql (Remove Duplicates)

```sql
-- models/silver/silver_customers_dedup.sql
-- Deduplicated customer records, keeping latest

{{ config(
    materialized='table',
    schema='silver',
    unique_key='customer_id',
    on_schema_change='fail',
    description='Deduplicated customer master'
) }}

WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY updated_at DESC, created_at DESC
    ) as rn
  FROM {{ ref('stg_customers') }}
)

SELECT 
  customer_id,
  customer_name,
  email,
  country,
  created_at,
  updated_at,
  _dbt_silver_at
FROM deduplicated
WHERE rn = 1
```

### schema.yml (Silver Tests)

```yaml
# models/silver/schema.yml
version: 2

models:
  - name: stg_customers
    description: Cleaned customer records
    columns:
      - name: customer_id
        tests:
          - not_null
          - unique
      - name: email
        tests:
          - unique
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

  - name: silver_customers_dedup
    description: Latest customer record per ID
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
```

**Why Silver Layer?**
- Centralized cleaning logic (reusable)
- Data quality gates (tests validate data)
- Ephemeral intermediate models (save storage)
- Deduplication happens once (efficiency)

---

## 6. Gold Layer: Analytics-Ready Fact & Dimensions

### fact_orders.sql (Gold Layer)

```sql
-- models/gold/fact_orders.sql
-- Order fact table with incremental load

{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns',
    partition_by={
      'field': 'order_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['customer_id', 'status'],
    indexes=[
      {'columns': ['customer_id'], 'type': 'hash'},
      {'columns': ['order_date'], 'type': 'btree'}
    ],
    schema='gold',
    tags=['gold', 'facts', 'critical'],
    description='Core fact table for order analytics'
) }}

SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  o.amount,
  UPPER(o.status) as status,
  CAST(DATE_TRUNC(o.order_date, MONTH) AS DATE) as order_month,
  EXTRACT(YEAR FROM o.order_date) as order_year,
  c.customer_name,
  c.country,
  c.customer_segment,
  CURRENT_TIMESTAMP() as _dbt_loaded_at
FROM {{ ref('stg_orders') }} o  -- silver layer
LEFT JOIN {{ ref('dim_customers') }} c
  ON o.customer_id = c.customer_id

{% if execute %}
  WHERE o.order_date >= 
    COALESCE(
      (SELECT MAX(order_date) FROM {{ this }}),
      CAST('2020-01-01' AS DATE)
    )
{% endif %}
```

**Why Incremental?**
- Only loads new orders since last run
- 90% faster than full refresh
- Reduces BigQuery scan costs
- Critical for large fact tables

### dim_customers.sql (Gold Dimension)

```sql
-- models/gold/dim_customers.sql
-- Customer dimension with SCD Type 1

{{ config(
    materialized='table',
    unique_key='customer_id',
    schema='gold',
    tags=['gold', 'dimensions'],
    description='Customer dimension with segmentation'
) }}

WITH customer_metrics AS (
  SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.amount) as lifetime_value,
    AVG(o.amount) as avg_order_value,
    MAX(o.order_date) as last_order_date
  FROM {{ ref('silver_customers_dedup') }} c
  LEFT JOIN {{ ref('fact_orders') }} o
    ON c.customer_id = o.customer_id
  GROUP BY c.customer_id, c.customer_name, c.email, c.country
)

SELECT
  customer_id,
  customer_name,
  email,
  country,
  total_orders,
  lifetime_value,
  avg_order_value,
  last_order_date,
  DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) as days_since_purchase,
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) <= 30 
      AND lifetime_value > 1000 THEN 'VIP'
    WHEN total_orders > 10 THEN 'Loyal'
    WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) > 180 THEN 'Inactive'
    ELSE 'Active'
  END as customer_segment,
  CURRENT_TIMESTAMP() as _dbt_updated_at
FROM customer_metrics
```

---

## 7. Cloud Composer DAG: DBT Orchestration

### dag_dbt_transform_silver.py (Silver DBT Run)

```python
# dags/dag_dbt_transform_silver.py
# Execute DBT transformations for silver layer

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_dbt_transform_silver',
    default_args=default_args,
    description='Run DBT transformations for silver layer',
    schedule_interval='0 3 * * *',  # 3 AM (after bronze loads at 2 AM)
    catchup=False,
    tags=['dbt', 'silver', 'daily'],
)

# Task 1: Install DBT dependencies
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt deps --profiles-dir /home/airflow/gcs/data/dbt_project
    ''',
    dag=dag
)

# Task 2: Parse DBT project (validate syntax)
dbt_parse = BashOperator(
    task_id='dbt_parse',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt parse --profiles-dir /home/airflow/gcs/data/dbt_project
    ''',
    dag=dag
)

# Task 3: Run DBT silver layer models
dbt_run_silver = BashOperator(
    task_id='dbt_run_silver',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt run \
      --select "path:models/silver/*" \
      --profiles-dir /home/airflow/gcs/data/dbt_project \
      --target prod \
      --threads 4
    ''',
    dag=dag
)

# Task 4: Test silver layer
dbt_test_silver = BashOperator(
    task_id='dbt_test_silver',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt test \
      --select "path:models/silver/*" \
      --profiles-dir /home/airflow/gcs/data/dbt_project
    ''',
    dag=dag
)

# Task 5: Log transformation metrics
def log_silver_metrics(**context):
    """Log silver layer transformation details"""
    from google.cloud import bigquery
    import logging
    
    client = bigquery.Client(project='project-id')
    logger = logging.getLogger(__name__)
    
    query = """
    SELECT
      'silver_customers_dedup' as table_name,
      COUNT(*) as row_count,
      CURRENT_TIMESTAMP() as processed_at
    FROM `project.silver.silver_customers_dedup`
    """
    
    results = client.query(query).result()
    for row in results:
        logger.info(f"Silver {row.table_name}: {row.row_count} rows")

log_metrics = PythonOperator(
    task_id='log_silver_metrics',
    python_callable=log_silver_metrics,
    provide_context=True,
    dag=dag
)

# Task dependencies
dbt_deps >> dbt_parse >> dbt_run_silver >> dbt_test_silver >> log_metrics
```

### dag_dbt_transform_gold.py (Gold DBT Run)

```python
# dags/dag_dbt_transform_gold.py
# Execute DBT transformations for gold layer (fact & dimension tables)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['analytics-team@company.com'],
    'email_on_failure': True,
}

dag = DAG(
    'dag_dbt_transform_gold',
    default_args=default_args,
    description='Run DBT for gold layer (analytics-ready tables)',
    schedule_interval='0 4 * * *',  # 4 AM (after silver completes at 3 AM)
    catchup=False,
    tags=['dbt', 'gold', 'analytics'],
)

# Task 1: Build gold layer fact tables
dbt_run_gold_facts = BashOperator(
    task_id='dbt_run_gold_facts',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt run \
      --select "tag:facts" \
      --profiles-dir /home/airflow/gcs/data/dbt_project \
      --target prod \
      --threads 6
    ''',
    dag=dag
)

# Task 2: Build gold layer dimension tables
dbt_run_gold_dims = BashOperator(
    task_id='dbt_run_gold_dims',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt run \
      --select "tag:dimensions" \
      --profiles-dir /home/airflow/gcs/data/dbt_project \
      --target prod \
      --threads 6
    ''',
    dag=dag
)

# Task 3: Create snapshots (SCD Type 2)
dbt_snapshot = BashOperator(
    task_id='dbt_snapshot',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt snapshot \
      --profiles-dir /home/airflow/gcs/data/dbt_project \
      --target prod
    ''',
    dag=dag
)

# Task 4: Test gold layer
dbt_test_gold = BashOperator(
    task_id='dbt_test_gold',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt test \
      --select "path:models/gold/*" \
      --profiles-dir /home/airflow/gcs/data/dbt_project
    ''',
    dag=dag
)

# Task 5: Generate documentation & lineage
dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='''
    cd /home/airflow/gcs/data/dbt_project &&
    dbt docs generate \
      --profiles-dir /home/airflow/gcs/data/dbt_project
    ''',
    dag=dag
)

# Task 6: Send success notification
send_success_email = EmailOperator(
    task_id='send_success_email',
    to=['analytics-team@company.com'],
    subject='Gold Layer Transformation Complete - {{ ds }}',
    html_content='''
    <h3>DBT Gold Layer Transformation Completed Successfully</h3>
    <p><strong>Date:</strong> {{ ds }}</p>
    <p><strong>Status:</strong> ✅ SUCCESS</p>
    <p>All gold layer tables updated and tested.</p>
    <p><a href="https://dbt-docs.company.com">View DBT Documentation</a></p>
    ''',
    dag=dag
)

# Task dependencies: Facts and dimensions run in parallel, then snapshot and tests
[dbt_run_gold_facts, dbt_run_gold_dims] >> dbt_snapshot >> dbt_test_gold >> dbt_docs >> send_success_email
```

**Why Cloud Composer?**
- Orchestrates multi-step pipelines (Bronze → Silver → Gold)
- Handles dependencies (Silver only runs after Bronze completes)
- Schedules daily runs
- Monitors SLAs and alerts on failures
- Enables parallel execution (facts & dims run simultaneously)
- Integrates with DBT CLI commands

---

## 8. Reconciliation Layer: Legacy vs New System

### dag_reconciliation.py (Validation & Dual-Run)

```python
# dags/dag_reconciliation.py
# Compare legacy system with new BQ warehouse

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta

dag = DAG(
    'dag_reconciliation_check',
    default_args={
        'owner': 'data-engineering',
        'retries': 2,
    },
    schedule_interval='0 5 * * *',  # 5 AM (after gold layer completes)
    catchup=False,
    tags=['reconciliation', 'validation'],
)

def reconcile_customer_counts(**context):
    """Compare customer counts: Legacy vs New Warehouse"""
    from google.cloud import bigquery
    import logging
    
    client = bigquery.Client(project='project-id')
    logger = logging.getLogger(__name__)
    
    # Query legacy system (via connector or federated query)
    legacy_query = """
    SELECT COUNT(*) as legacy_count
    FROM `project.legacy_federation.customers_view`
    """
    
    # Query new warehouse
    new_query = """
    SELECT COUNT(*) as new_count
    FROM `project.gold.dim_customers`
    """
    
    legacy_result = list(client.query(legacy_query).result())[0].legacy_count
    new_result = list(client.query(new_query).result())[0].new_count
    
    discrepancy_pct = abs(legacy_result - new_result) / legacy_result * 100
    
    logger.info(f"Legacy: {legacy_result} | New: {new_result} | Variance: {discrepancy_pct}%")
    
    if discrepancy_pct > 0.1:  # > 0.1% discrepancy is failure
        raise ValueError(f"Reconciliation failed: {discrepancy_pct}% variance")
    
    context['task_instance'].xcom_push(
        key='reconciliation_result',
        value={
            'legacy_count': legacy_result,
            'new_count': new_result,
            'variance_pct': discrepancy_pct,
            'status': 'PASSED'
        }
    )

reconcile_customers = PythonOperator(
    task_id='reconcile_customer_counts',
    python_callable=reconcile_customer_counts,
    provide_context=True,
    dag=dag
)

def reconcile_order_amounts(**context):
    """Compare total order amounts"""
    from google.cloud import bigquery
    
    client = bigquery.Client(project='project-id')
    
    query = """
    WITH legacy AS (
      SELECT SUM(amount) as total FROM `project.legacy_federation.orders_view`
    ),
    new_wh AS (
      SELECT SUM(amount) as total FROM `project.gold.fact_orders`
    )
    SELECT
      l.total as legacy_total,
      n.total as new_total,
      ROUND(ABS(l.total - n.total), 2) as variance
    FROM legacy l, new_wh n
    """
    
    result = list(client.query(query).result())[0]
    
    if result.variance > 0.01:  # > 1 cent variance fails
        raise ValueError(f"Amount variance: {result.variance}")
    
    context['task_instance'].xcom_push(
        key='amount_reconciliation',
        value={
            'legacy_total': float(result.legacy_total),
            'new_total': float(result.new_total),
            'status': 'PASSED'
        }
    )

reconcile_amounts = PythonOperator(
    task_id='reconcile_order_amounts',
    python_callable=reconcile_amounts,
    provide_context=True,
    dag=dag
)

def send_reconciliation_report(**context):
    """Generate and send reconciliation report"""
    customer_reconciliation = context['task_instance'].xcom_pull(
        task_ids='reconcile_customer_counts',
        key='reconciliation_result'
    )
    amount_reconciliation = context['task_instance'].xcom_pull(
        task_ids='reconcile_order_amounts',
        key='amount_reconciliation'
    )
    
    report = {
        'execution_date': context['execution_date'],
        'customer_check': customer_reconciliation,
        'amount_check': amount_reconciliation,
        'overall_status': 'PASSED'
    }
    
    context['task_instance'].xcom_push(
        key='reconciliation_report',
        value=report
    )

generate_report = PythonOperator(
    task_id='generate_reconciliation_report',
    python_callable=send_reconciliation_report,
    provide_context=True,
    dag=dag
)

# Send Slack alert if any reconciliation fails
slack_alert = SlackWebhookOperator(
    task_id='slack_reconciliation_alert',
    http_conn_id='slack_webhook',
    message='✅ Daily reconciliation passed! Legacy vs New Warehouse match.',
    trigger_rule='all_success',
    dag=dag
)

reconcile_customers >> reconcile_amounts >> generate_report >> slack_alert
```

**Why Reconciliation Layer?**
- Validates migration completeness
- Ensures data accuracy during transition
- Dual-run period safety net
- Row counts, amounts, key lookups must match
- Catch bugs before analytics sees wrong data

---

## 9. Data Flow Timing: Why This Sequence?

```
Timeline:
┌──────────────────────────────────────────────────────────────────┐
│                    DAILY EXECUTION SCHEDULE                      │
└──────────────────────────────────────────────────────────────────┘

2:00 AM → dag_ingestion_bronze
├── Extract from Oracle/MySQL/APIs
├── Load to bronze.raw_* (exact copies)
├── Validate row counts & nulls
└── Status: Ready for transformation

3:00 AM → dag_dbt_transform_silver
├── Reads: bronze.raw_* tables
├── Creates: silver.stg_*, silver_*_dedup tables
├── Deduplicates & cleans data
├── Tests: uniqueness, nulls, formats
└── Status: Clean, deduplicated data ready

4:00 AM → dag_dbt_transform_gold
├── Reads: silver.* tables
├── Creates: gold.fact_*, gold.dim_* (analytics-ready)
├── Incremental loads (only new records)
├── Tests: relationships, freshness
├── Generates: dbt docs & lineage
└── Status: Analytics tables updated

5:00 AM → dag_reconciliation_check
├── Compares: Legacy vs BigQuery
├── Validates: Counts match, amounts match
├── Tests: Referential integrity
└── Status: ✅ Data accuracy confirmed

5:30 AM → BI Tools Refresh
├── Looker/Tableau query gold.fact_* & gold.dim_*
├── Dashboards reflect latest data
└── Status: Analytics ready for users

DAY → Analysts use gold tables for:
├── Ad-hoc SQL queries
├── BI dashboards & reports
├── Data science & ML features
└── Downstream applications

Why This Order?
✅ Bronze first = Raw data captured before transformation
✅ Silver second = Centralized cleaning (efficient)
✅ Gold third = Analytics tables build on clean data
✅ Tests run = Catch bugs at each layer
✅ Reconciliation last = Verify everything matches legacy
✅ Sequential (not parallel) = Avoid transformation of incomplete data
```

---

## 10. Why Cloud Composer? (vs Cron/Manual Scheduling)

```
Cron Scheduling (Old Way):
❌ No dependency management
❌ No error retries
❌ No visibility into failures
❌ Manual debugging required
❌ Can't run tasks in parallel
❌ No SLA monitoring
❌ Scripts fail silently

Cloud Composer (New Way):
✅ Orchestrate complex dependencies
✅ Automatic retries with backoff
✅ Real-time monitoring dashboard
✅ Email/Slack alerts on failure
✅ Parallel task execution
✅ SLA tracking & enforcement
✅ Historical run logs
✅ Integrates with GCP services
✅ Scales to thousands of tasks
✅ Version control via GIT
```

---

## 11. Why DBT + Composer Together?

```
DBT handles:
- ✅ SQL transformation logic
- ✅ Testing & validation
- ✅ Documentation
- ✅ Lineage tracking
- ✅ Version control

Cloud Composer handles:
- ✅ Scheduling
- ✅ Orchestration
- ✅ Error handling & retries
- ✅ Monitoring & alerting
- ✅ SLA enforcement
- ✅ Cross-system coordination

Example: dbt run fails → Composer retries → Slack alert → Dashboard shows stale data status
```

---

## 12. Cost Optimization Strategies

### Bronze Layer
```sql
-- Partition by ingestion_date + TTL policy
CREATE TABLE `project.bronze.raw_orders`
PARTITION BY DATE(ingestion_date)
CLUSTER BY customer_id
OPTIONS(
  partition_expiration_ms = 63072000000,  -- 2 years
  description = "Raw orders with 2-year retention"
);

-- Estimated cost: $5-10/month (compressed raw data)
```

### Silver Layer
```sql
-- Ephemeral models = No storage cost
-- Only materialized dedup table stored
-- 30% smaller than bronze (after cleaning)

-- Estimated cost: $2-5/month
```

### Gold Layer
```sql
-- Incremental facts = Process only new data
-- Clustering on common joins = Faster queries
-- Materialized views for expensive aggregations

-- Estimated cost: $10-20/month (depends on query patterns)
```

**Total Cost per Month:** ~$20-40 vs $200+ with legacy manual SQL approach

---

## 13. Migration Phases

```
PHASE 1: PARALLEL RUN (Weeks 1-4)
├── Legacy system running live
├── New warehouse built in shadow
├── Reconciliation checks validate accuracy
├── Zero risk (no customer impact)
└── Cost: 2x storage temporarily

PHASE 2: READ-ONLY CUTOVER (Week 5)
├── BI tools switched to BigQuery
├── Legacy system read-only
├── Monitor for issues
├── Rollback ready if needed
└── Cost: 1.5x storage

PHASE 3: DECOMMISSION (Week 6+)
├── Archive legacy system
├── Keep only for audit/compliance
├── Reduce infrastructure cost
└── Cost: 1x storage (BigQuery only)
```

---

## 14. Complete End-to-End Example

### Question: "Build a report of top 10 customers by revenue"

#### Legacy Approach (Problem)
```sql
-- Query 1: Extract from Oracle
-- Query 2: Manual join in Python
-- Query 3: Load to staging table
-- Query 4: Transform in BigQuery
-- Query 5: Create report query
-- Result: Fragmented, hard to test, no lineage
```

#### New Approach (Solution with DBT+Composer)

**Day 1: Data Ingestion (Cloud Composer)**
```
dag_ingestion_bronze →
  Load raw_customers to bronze.raw_customers
  Load raw_orders to bronze.raw_orders
```

**Day 1 (3 AM): Silver Cleaning (DBT + Composer)**
```sql
-- models/silver/stg_customers.sql
-- models/silver/stg_orders.sql
-- Tests validate data quality
-- dbt test passes ✅
```

**Day 1 (4 AM): Gold Aggregation (DBT + Composer)**
```sql
-- models/gold/fact_orders.sql
-- models/gold/dim_customers.sql
-- models/gold/agg_top_customers.sql

CREATE OR REPLACE TABLE gold.agg_top_customers AS
SELECT
  c.customer_id,
  c.customer_name,
  c.country,
  COUNT(o.order_id) as order_count,
  SUM(o.amount) as total_revenue,
  ROW_NUMBER() OVER (ORDER BY SUM(o.amount) DESC) as rank
FROM gold.dim_customers c
LEFT JOIN gold.fact_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.country
HAVING ROW_NUMBER() OVER (ORDER BY SUM(o.amount) DESC) <= 10
```

**Day 1 (5 AM): Reconciliation (Cloud Composer)**
```
dag_reconciliation_check →
  Compare counts: Legacy vs BigQuery ✅
  Compare amounts: Legacy vs BigQuery ✅
```

**Day 1 (5:30 AM): BI Refresh (Looker)**
```
Looker → SELECT * FROM gold.agg_top_customers
```

**Result:**
- ✅ Single source of truth (gold.agg_top_customers)
- ✅ Versioned & tested (dbt git history)
- ✅ Documented (auto-generated lineage)
- ✅ Repeatable (runs daily automatically)
- ✅ Auditable (knows exactly where data came from)

---

## 15. Monitoring & Alerts

### Cloud Composer DAG Monitoring
```
Dashboard shows:
├── Run status (success/failed)
├── Duration (trend over time)
├── Retry count (if any)
├── Task-level execution times
├── Error messages
└── Data quality metrics
```

### DBT Test Monitoring
```python
# Parse dbt_cloud API for test results
dbt_test_summary:
├── Total tests: 150
├── Passed: 148 (98.7%)
├── Failed: 2 (1.3%)
├── Skipped: 0
└── Alert if any failures
```

### Sample Alert (Slack)
```
⚠️ Data Pipeline Alert

❌ dag_dbt_transform_gold failed at task: dbt_test_gold
Task: dbt_test_gold
Error: Uniqueness test failed on gold.dim_customers.customer_id

Possible causes:
- Duplicate customer records in silver layer
- Bug in stg_customers transformation

Action: Check dbt test results
Link: [Composer Dashboard]
Time: 2024-01-15 04:23 UTC
```

---

## 16. Key Learnings: Why This Architecture?

| Component | Purpose | Benefit |
|-----------|---------|---------|
| **Bronze** | Raw data capture | Audit trail, restart point |
| **Silver** | Data cleaning | Reusable, tested, deduplicated |
| **Gold** | Analytics-ready | Fast queries, documented, governed |
| **DBT** | SQL versioning | Git history, testing, lineage |
| **Composer** | Orchestration | Scheduling, monitoring, retries |
| **Tests** | Quality gates | Catch bugs early, prevent bad data |
| **Incremental** | Performance | Only process new data |
| **Partitioning** | Cost optimization | Prune irrelevant data scans |

---

## Summary: Complete Data Flow

```
Legacy Sources → Bronze (Raw) → Silver (Cleaned) → Gold (Analytics)
                     ↓              ↓                  ↓
                 [Ingest]      [DBT Clean]        [DBT Transform]
                   via           via                  via
                Composer        DBT+Composer        DBT+Composer
                     ↓              ↓                  ↓
                BigQuery        BigQuery            BigQuery
               raw_* tables     dedup tables       fact_* & dim_*
                     ↓              ↓                  ↓
                [Cloud Composer Orchestrates All 3 Layers]
                     ↓              ↓                  ↓
              Reconciliation → Tests → Monitoring → Alerts
                     ↓
                   [Gold Tables Ready for BI/Analytics]
```

**Migration Success = Medallion Architecture + DBT + Cloud Composer + Comprehensive Testing**