# Cloud Composer DAG Flow - Complete Guide

## 1. Basic DAG Structure & Initialization

### Imports & DAG Definition
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# DAG default arguments - baseline configuration for all tasks
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,  # Don't fail if previous run failed
    'start_date': days_ago(1),  # Start from 1 day ago
    'email': ['alerts@company.com'],  # Alert recipients
    'email_on_failure': True,  # Send alerts on failure
    'email_on_retry': False,  # Don't alert on retry
    'retries': 2,  # Retry failed tasks 2 times
    'retry_delay': timedelta(minutes=5),  # Wait 5 min before retry
    'execution_timeout': timedelta(hours=2),  # Max task runtime
    'pool': 'default_pool',  # Resource pool limiting concurrent tasks
}

# DAG definition with metadata
dag = DAG(
    dag_id='data_pipeline_etl_flow',  # Unique DAG identifier
    default_args=default_args,
    description='End-to-end data pipeline: ingest, transform, validate, export',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM UTC
    catchup=False,  # Don't backfill missed runs
    max_active_runs=1,  # Only 1 concurrent DAG run
    tags=['data-engineering', 'production'],  # For organization
)
```

**Explanation:**
- `default_args`: Base configuration inherited by all tasks
- `schedule_interval`: Cron format (minute hour day month dayofweek)
- `catchup=False`: Skip historical runs if DAG wasn't active
- `max_active_runs`: Prevent simultaneous runs on same DAG

---

## 2. Data Ingestion Layer

### Task 1: Check GCS Data Availability
```python
check_gcs_files = GCSListObjectsOperator(
    task_id='check_gcs_files',
    bucket='raw-data-bucket',  # GCS bucket name
    prefix='imports/customer_data/',  # Directory path
    delimiter='/',
    do_xcom_push=True,  # Push result to XCom for downstream tasks
    dag=dag
)
```

**Explanation:**
- Validates raw data exists in Cloud Storage before processing
- `do_xcom_push=True`: Stores file list for next task access

### Task 2: Load CSV from GCS to BigQuery (Staging)
```python
load_gcs_to_staging = GCSToBigQueryOperator(
    task_id='load_raw_to_staging',
    bucket='raw-data-bucket',
    source_objects=['imports/customer_data/*.csv'],  # Wildcard for multiple files
    destination_project_dataset_table='project.staging.raw_customers',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',  # Overwrite staging table
    autodetect=False,
    schema=[  # Define schema explicitly
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'load_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    ],
    dag=dag
)
```

**Explanation:**
- Directly transfers CSV from GCS to BigQuery staging table
- `WRITE_TRUNCATE`: Clears old data; use `WRITE_APPEND` for incremental
- Schema definition prevents type mismatches

---

## 3. Data Validation Layer

### Task 3: Validate Raw Data Quality
```python
def validate_raw_data(**context):
    """
    Custom Python function for data validation
    - Check row counts
    - Detect nulls
    - Validate formats
    """
    from google.cloud import bigquery
    
    client = bigquery.Client(project='project-id')
    
    # Query to check data quality
    query = """
    SELECT
      COUNT(*) as total_rows,
      COUNTIF(customer_id IS NULL) as null_customer_ids,
      COUNTIF(NOT REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@')) as invalid_emails
    FROM `project.staging.raw_customers`
    """
    
    results = client.query(query).result()
    for row in results:
        print(f"Total rows: {row.total_rows}")
        print(f"Null IDs: {row.null_customer_ids}")
        print(f"Invalid emails: {row.invalid_emails}")
        
        # Fail if too many nulls (> 10%)
        if row.null_customer_ids > (row.total_rows * 0.1):
            raise ValueError("Data quality check failed: too many null customer IDs")
        
        # Push metrics to XCom for monitoring
        context['task_instance'].xcom_push(
            key='validation_results',
            value={
                'total_rows': row.total_rows,
                'null_count': row.null_customer_ids,
                'status': 'PASSED'
            }
        )

validate_data = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data,
    provide_context=True,  # Pass Airflow context object
    dag=dag
)
```

**Explanation:**
- Validates data quality before processing
- `xcom_push`: Stores results for downstream monitoring
- Raises exception to fail task if quality issues detected

### Task 4: Conditional Execution (Branch Operator)
```python
from airflow.operators.python import BranchPythonOperator

def decide_processing_path(**context):
    """
    Branching logic: route to different tasks based on data quality
    """
    # Get validation results from upstream task
    validation_results = context['task_instance'].xcom_pull(
        task_ids='validate_raw_data',
        key='validation_results'
    )
    
    if validation_results['status'] == 'PASSED':
        return 'transform_data'  # Proceed to transformation
    else:
        return 'send_alert_email'  # Alert if quality issues

branch_task = BranchPythonOperator(
    task_id='decide_next_step',
    python_callable=decide_processing_path,
    provide_context=True,
    dag=dag
)
```

**Explanation:**
- Makes dynamic decisions based on upstream results
- Returns task_id to execute next
- Enables conditional pipelines (success vs error paths)

---

## 4. Data Transformation Layer

### Task 5: Data Cleaning & Standardization
```python
clean_and_standardize = BigQueryInsertJobOperator(
    task_id='clean_and_standardize',
    configuration={
        'query': {
            'query': """
            CREATE OR REPLACE TABLE `project.processing.customers_cleaned` AS
            SELECT
              UPPER(TRIM(customer_id)) as customer_id,
              UPPER(TRIM(customer_name)) as customer_name,
              LOWER(TRIM(email)) as email,
              CASE 
                WHEN country IS NULL THEN 'Unknown'
                ELSE UPPER(TRIM(country)) 
              END as country,
              CURRENT_TIMESTAMP() as processed_at
            FROM `project.staging.raw_customers`
            WHERE customer_id IS NOT NULL
              AND REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
            """,
            'useLegacySql': False,
            'priority': 'INTERACTIVE',  # INTERACTIVE (faster) or BATCH (cheaper)
            'maximumBytesBilled': '1000000000',  # $5 cost limit
        }
    },
    location='US',  # BigQuery dataset location
    dag=dag
)
```

**Explanation:**
- Cleans data: trim, uppercase, lowercase, null handling
- Filters invalid records before transformation
- `maximumBytesBilled`: Cost control to prevent expensive queries

### Task 6: Deduplication & Golden Records
```python
create_golden_records = BigQueryInsertJobOperator(
    task_id='create_golden_records',
    configuration={
        'query': {
            'query': """
            CREATE OR REPLACE TABLE `project.warehouse.customers` AS
            WITH deduplicated AS (
              SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY processed_at DESC) as rn
              FROM `project.processing.customers_cleaned`
            )
            SELECT * EXCEPT(rn)
            FROM deduplicated
            WHERE rn = 1
            """,
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag
)
```

**Explanation:**
- Removes duplicates, keeps latest record
- Creates "golden record" version in warehouse
- `ROW_NUMBER() with ORDER BY processed_at DESC`: Selects newest

### Task 7: Feature Engineering for ML
```python
engineer_features = BigQueryInsertJobOperator(
    task_id='engineer_features',
    configuration={
        'query': {
            'query': """
            CREATE OR REPLACE TABLE `project.ml_features.customer_features` AS
            WITH customer_orders AS (
              SELECT
                c.customer_id,
                c.customer_name,
                COUNT(DISTINCT o.order_id) as total_orders,
                SUM(o.amount) as lifetime_value,
                AVG(o.amount) as avg_order_value,
                MAX(o.order_date) as last_purchase_date,
                DATE_DIFF(CURRENT_DATE(), MAX(o.order_date), DAY) as days_since_purchase
              FROM `project.warehouse.customers` c
              LEFT JOIN `project.warehouse.orders` o ON c.customer_id = o.customer_id
              GROUP BY c.customer_id, c.customer_name
            )
            SELECT
              *,
              CASE 
                WHEN total_orders = 0 THEN 'Never_Purchased'
                WHEN days_since_purchase > 365 THEN 'Inactive'
                WHEN days_since_purchase <= 30 THEN 'Active'
                ELSE 'At_Risk'
              END as customer_status
            FROM customer_orders
            """,
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag
)
```

**Explanation:**
- Aggregates customer behavior metrics
- Creates derived features for ML models
- Segments customers based on recency

---

## 5. Data Enrichment Layer

### Task 8: Join with Master Reference Data
```python
enrich_with_reference = BigQueryInsertJobOperator(
    task_id='enrich_with_reference_data',
    configuration={
        'query': {
            'query': """
            CREATE OR REPLACE TABLE `project.warehouse.customers_enriched` AS
            SELECT
              c.customer_id,
              c.customer_name,
              c.email,
              c.country,
              g.country_name,
              g.region,
              g.timezone,
              cf.total_orders,
              cf.lifetime_value,
              cf.customer_status
            FROM `project.warehouse.customers` c
            LEFT JOIN `project.warehouse.geo_reference` g 
              ON c.country = g.country_code
            LEFT JOIN `project.ml_features.customer_features` cf 
              ON c.customer_id = cf.customer_id
            """,
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag
)
```

**Explanation:**
- Joins customer data with reference tables (geography, lookup)
- Enriches with context data for analytics
- Uses LEFT JOIN to preserve all customers

---

## 6. Data Aggregation & Reporting

### Task 9: Generate Daily Summary Report
```python
generate_summary_report = BigQueryInsertJobOperator(
    task_id='generate_daily_summary',
    configuration={
        'query': {
            'query': """
            CREATE OR REPLACE TABLE `project.reports.daily_customer_summary` 
            PARTITION BY report_date AS
            SELECT
              CURRENT_DATE() as report_date,
              COUNT(DISTINCT customer_id) as total_customers,
              COUNTIF(customer_status = 'Active') as active_customers,
              COUNTIF(customer_status = 'Inactive') as inactive_customers,
              SUM(lifetime_value) as total_revenue,
              AVG(lifetime_value) as avg_customer_value,
              MAX(lifetime_value) as max_customer_value,
              CURRENT_TIMESTAMP() as generated_at
            FROM `project.ml_features.customer_features`
            """,
            'useLegacySql': False,
            'timePartitioningType': 'DAY',  # Auto-partition by date
        }
    },
    location='US',
    dag=dag
)
```

**Explanation:**
- Creates daily snapshot of key metrics
- Auto-partitioning improves query performance
- Report ready for BI dashboards (Looker, Tableau)

---

## 7. Data Quality Monitoring

### Task 10: Quality Assertions
```python
def quality_assertions(**context):
    """
    Post-transformation quality checks
    Validates transformed data meets requirements
    """
    from google.cloud import bigquery
    import json
    
    client = bigquery.Client(project='project-id')
    assertions = {}
    
    # Check 1: No duplicate customer IDs
    query1 = """
    SELECT COUNT(*) as dup_count
    FROM (
      SELECT customer_id, COUNT(*) as cnt
      FROM `project.warehouse.customers`
      GROUP BY customer_id
      HAVING cnt > 1
    )
    """
    result = client.query(query1).result()
    dup_count = list(result)[0].dup_count
    assertions['duplicates'] = 'PASS' if dup_count == 0 else 'FAIL'
    
    # Check 2: Minimum row count (sanity check)
    query2 = "SELECT COUNT(*) as row_count FROM `project.warehouse.customers`"
    result = client.query(query2).result()
    row_count = list(result)[0].row_count
    assertions['min_rows'] = 'PASS' if row_count > 1000 else 'FAIL'
    
    # Check 3: Email format validation
    query3 = """
    SELECT COUNT(*) as invalid_count
    FROM `project.warehouse.customers`
    WHERE NOT REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@')
    """
    result = client.query(query3).result()
    invalid_count = list(result)[0].invalid_count
    assertions['email_format'] = 'PASS' if invalid_count == 0 else 'FAIL'
    
    # Log results
    print(f"Quality Assertions: {json.dumps(assertions, indent=2)}")
    
    # Push to monitoring system
    context['task_instance'].xcom_push(
        key='quality_assertions',
        value=assertions
    )
    
    # Fail if any assertion fails
    if 'FAIL' in assertions.values():
        raise ValueError(f"Quality assertions failed: {assertions}")

quality_check = PythonOperator(
    task_id='quality_assertions',
    python_callable=quality_assertions,
    provide_context=True,
    dag=dag
)
```

**Explanation:**
- Post-transformation validation
- Checks duplicates, row counts, format compliance
- Fails pipeline if quality issues found

---

## 8. Data Export & Distribution

### Task 11: Export to Cloud Storage
```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator

export_to_gcs = BigQueryInsertJobOperator(
    task_id='export_customers_to_gcs',
    configuration={
        'extract': {
            'sourceTable': {
                'projectId': 'project-id',
                'datasetId': 'warehouse',
                'tableId': 'customers_enriched'
            },
            'destinationUris': ['gs://analytics-bucket/exports/customers_*.parquet'],
            'destinationFormat': 'PARQUET',  # Optimized columnar format
            'compression': 'SNAPPY',  # Compression codec
        }
    },
    location='US',
    dag=dag
)
```

**Explanation:**
- Exports BigQuery data to Parquet format in GCS
- Optimized for BI tools (Tableau, Looker, Power BI)
- Partitions across multiple files with `_*`

### Task 12: Send Data to External System (API)
```python
from airflow.operators.http_operator import SimpleHttpOperator

send_to_api = SimpleHttpOperator(
    task_id='send_metrics_to_analytics_api',
    http_conn_id='analytics_api',  # Configured in Airflow UI
    endpoint='/api/v1/daily-metrics',
    method='POST',
    data='{"date": "{{ ds }}", "metrics": "{{ ti.xcom_pull(task_ids=\'generate_daily_summary\') }}"}',
    headers={'Content-Type': 'application/json'},
    dag=dag
)
```

**Explanation:**
- POST data to external APIs
- Uses `ds` (datestamp) template variable
- XCom pulls upstream results

---

## 9. Notification & Alerting

### Task 13: Send Success Email
```python
from airflow.operators.email import EmailOperator

send_success_email = EmailOperator(
    task_id='send_pipeline_success_email',
    to=['data-team@company.com'],
    subject='Data Pipeline Completed Successfully - {{ ds }}',
    html_content="""
    <h3>Data Pipeline Execution Report</h3>
    <p><strong>Date:</strong> {{ ds }}</p>
    <p><strong>Status:</strong> SUCCESS</p>
    <p><strong>Records Processed:</strong> {{ ti.xcom_pull(task_ids='generate_daily_summary', key='row_count') }}</p>
    <p><strong>Duration:</strong> {{ execution_date }}</p>
    <p>All data quality checks passed. Data is ready for analytics.</p>
    """,
    dag=dag
)
```

**Explanation:**
- Sends email on success
- Templated with execution date
- Pulls metrics from upstream tasks via XCom

### Task 14: Slack Notification on Failure
```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

slack_alert = SlackWebhookOperator(
    task_id='slack_failure_alert',
    http_conn_id='slack_webhook',
    message='Data Pipeline Failed! Check Airflow logs.',
    trigger_rule='one_failed',  # Triggers when any upstream fails
    dag=dag
)
```

**Explanation:**
- Sends Slack alert on pipeline failure
- `trigger_rule='one_failed'`: Always runs if any task fails
- Real-time notifications to engineering team

---

## 10. Monitoring & Logging

### Task 15: Log Pipeline Metadata
```python
def log_pipeline_metadata(**context):
    """
    Logs execution metadata for monitoring and auditing
    Tracks data lineage and performance metrics
    """
    from datetime import datetime
    from google.cloud import bigquery
    
    client = bigquery.Client(project='project-id')
    
    # Build metadata record
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    
    # Query to log stats
    insert_query = f"""
    INSERT INTO `project.monitoring.pipeline_log` 
    (execution_date, dag_id, run_id, status, rows_processed, runtime_minutes, log_timestamp)
    SELECT
      '{execution_date}',
      '{dag_id}',
      '{run_id}',
      'COMPLETED',
      COUNT(*),
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), '{execution_date}', MINUTE),
      CURRENT_TIMESTAMP()
    FROM `project.warehouse.customers`
    """
    
    client.query(insert_query).result()
    print(f"Pipeline metadata logged for run: {run_id}")

log_metadata = PythonOperator(
    task_id='log_pipeline_metadata',
    python_callable=log_pipeline_metadata,
    provide_context=True,
    dag=dag
)
```

**Explanation:**
- Records execution stats in audit table
- Tracks row counts, runtime, status
- Enables data lineage tracking

---

## 11. Complete DAG Task Dependencies

### Define Task Flow
```python
# Linear flow with validation
check_gcs_files >> load_gcs_to_staging >> validate_data

# Branching: conditional execution
validate_data >> branch_task >> [
    transform_data >> clean_and_standardize >> create_golden_records,
    send_alert_email  # Alert path if validation fails
]

# Enrichment pipeline
create_golden_records >> engineer_features >> enrich_with_reference

# Reporting & exports
enrich_with_reference >> generate_summary_report >> [
    export_to_gcs,
    send_to_api,
    quality_check
]

# Final notifications
quality_check >> [send_success_email, log_metadata]
```

**Explanation:**
- `>>`: Dependency operator (sequential execution)
- `[task1, task2]`: Parallel execution
- Branching creates conditional paths

---

## 12. Advanced Patterns

### Task 16: Dynamic Task Generation
```python
from airflow.models import Variable

def generate_dynamic_tasks(**context):
    """
    Creates tasks dynamically based on configuration
    Enables scalable pipelines without code changes
    """
    tables = Variable.get('tables_to_process', deserialize_json=True)
    # {'tables': ['orders', 'customers', 'products']}
    
    for table in tables['tables']:
        print(f"Processing table: {table}")

# Get config from Variable
dynamic_config = PythonOperator(
    task_id='load_dynamic_config',
    python_callable=generate_dynamic_tasks,
    provide_context=True,
    dag=dag
)
```

**Explanation:**
- Reads table list from Airflow Variables
- Enables config-driven pipelines
- No code changes needed for new tables

### Task 17: Retry with Exponential Backoff
```python
from airflow.utils.decorators import apply_defaults

resilient_task = BigQueryInsertJobOperator(
    task_id='resilient_query',
    configuration={'query': {'query': 'SELECT COUNT(*) FROM table'}},
    location='US',
    execution_timeout=timedelta(hours=1),
    retries=3,  # Retry up to 3 times
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,  # 2min, 4min, 8min
    max_retry_delay=timedelta(minutes=30),  # Cap at 30min
    dag=dag
)
```

**Explanation:**
- Exponential backoff: 2min → 4min → 8min between retries
- Handles transient failures (API throttling, timeouts)
- Max retry delay prevents infinite backoff

### Task 18: SLA (Service Level Agreement) Monitoring
```python
dag = DAG(
    dag_id='monitored_pipeline',
    default_args={
        **default_args,
        'sla': timedelta(hours=3),  # DAG must complete in 3 hours
    },
    sla_miss_callback=lambda context: print("SLA MISSED!"),
    dag=dag
)

monitored_task = BigQueryInsertJobOperator(
    task_id='sla_monitored_task',
    configuration={'query': {'query': 'SELECT 1'}},
    location='US',
    sla=timedelta(hours=1),  # Task-level SLA
    dag=dag
)
```

**Explanation:**
- DAG-level SLA: entire pipeline must finish by deadline
- Task-level SLA: individual task timeout
- Alerts on SLA misses

---

## 13. Testing & Validation

### Task 19: Unit Test for DAG
```python
import unittest
from airflow.models import DAG
from datetime import datetime

class TestDataPipelineDAG(unittest.TestCase):
    
    def setUp(self):
        self.dag = dag
    
    def test_dag_loads(self):
        """Verify DAG loads without errors"""
        self.assertIsNotNone(self.dag)
    
    def test_task_exists(self):
        """Check required tasks exist"""
        task_ids = [t.task_id for t in self.dag.tasks]
        self.assertIn('load_raw_to_staging', task_ids)
        self.assertIn('validate_raw_data', task_ids)
    
    def test_dependencies(self):
        """Verify task dependencies are correct"""
        task = self.dag.get_task('clean_and_standardize')
        upstream_ids = [t.task_id for t in task.upstream_list]
        self.assertIn('validate_raw_data', upstream_ids)
    
    def test_default_args(self):
        """Validate default arguments"""
        self.assertEqual(self.dag.default_args['owner'], 'data-engineering-team')
        self.assertEqual(self.dag.default_args['retries'], 2)

if __name__ == '__main__':
    unittest.main()
```

**Explanation:**
- Unit tests for DAG structure
- Validates tasks and dependencies
- Catches configuration errors early

---

## 14. Incremental Processing Pattern

### Task 20: Incremental Data Sync
```python
def get_watermark(**context):
    """
    Retrieves last processed timestamp
    Enables incremental loading
    """
    from google.cloud import bigquery
    
    client = bigquery.Client(project='project-id')
    query = "SELECT MAX(processed_at) as last_sync FROM `project.warehouse.customers`"
    result = client.query(query).result()
    last_sync = list(result)[0].last_sync or '1970-01-01'
    
    # Push to XCom for downstream task
    context['task_instance'].xcom_push(
        key='last_watermark',
        value=str(last_sync)
    )
    return str(last_sync)

get_watermark_task = PythonOperator(
    task_id='get_last_watermark',
    python_callable=get_watermark,
    provide_context=True,
    dag=dag
)

# Use watermark in next query
incremental_load = BigQueryInsertJobOperator(
    task_id='incremental_sync',
    configuration={
        'query': {
            'query': """
            CREATE OR REPLACE TABLE `project.processing.incremental_batch` AS
            SELECT * FROM `project.staging.raw_customers`
            WHERE updated_at > '{{ ti.xcom_pull(task_ids="get_last_watermark", key="last_watermark") }}'
            """,
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag
)

get_watermark_task >> incremental_load
```

**Explanation:**
- Gets last processed timestamp from warehouse
- Queries only new/updated records since then
- Reduces data volume and processing time

---

## 15. Error Handling & Graceful Degradation

### Task 21: Try-Catch Pattern
```python
def safe_transform(**context):
    """
    Transform with error handling
    Logs errors and continues with fallback
    """
    from google.cloud import bigquery
    import logging
    
    logger = logging.getLogger(__name__)
    client = bigquery.Client(project='project-id')
    
    try:
        query = """
        SELECT * FROM `project.warehouse.customers`
        WHERE country IN (SELECT country FROM `project.reference.valid_countries`)
        """
        result = client.query(query).result()
        return result
    
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
        
        # Fallback: use unfiltered data
        fallback_query = "SELECT * FROM `project.warehouse.customers`"
        result = client.query(fallback_query).result()
        
        context['task_instance'].xcom_push(
            key='error_fallback',
            value={'error': str(e), 'used_fallback': True}
        )
        return result

safe_transform_task = PythonOperator(
    task_id='safe_transform',
    python_callable=safe_transform,
    provide_context=True,
    on_failure_callback=lambda context: print("Task failed, alerting team"),
    dag=dag
)
```

**Explanation:**
- Try-catch for graceful error handling
- Fallback logic for resilience
- Logging for debugging

---

## 16. Performance Optimization

### Task 22: Parallel Processing with XCom
```python
def process_batch_1(**context):
    """Process customer segment 1"""
    return {'segment': 'A-M', 'rows': 50000, 'duration': 120}

def process_batch_2(**context):
    """Process customer segment 2"""
    return {'segment': 'N-Z', 'rows': 55000, 'duration': 130}

def merge_batches(**context):
    """Merge results from parallel tasks"""
    batch1 = context['task_instance'].xcom_pull(task_ids='process_batch_1')
    batch2 = context['task_instance'].xcom_pull(task_ids='process_batch_2')
    
    total_rows = batch1['rows'] + batch2['rows']
    total_duration = max(batch1['duration'], batch2['duration'])
    print(f"Processed {total_rows} rows in {total_duration} seconds")

batch_task_1 = PythonOperator(task_id='process_batch_1', python_callable=process_batch_1, dag=dag)
batch_task_2 = PythonOperator(task_id='process_batch_2', python_callable=process_batch_2, dag=dag)
merge_task = PythonOperator(task_id='merge_batches', python_callable=merge_batches, provide_context=True, dag=dag)

# Run batches in parallel, then merge
[batch_task_1, batch_task_2] >> merge_task
```

**Explanation:**
- Parallel task execution reduces total runtime
- XCom passes intermediate results
- Merge combines results from parallel branches

---

## 17. Complete DAG Summary Diagram

```
                    ┌─────────────────────┐
                    │  Check GCS Files    │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │ Load GCS to Staging │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼────────────┐
                    │  Validate Raw Data    │
                    └──────────┬────────────┘
                               │
                    ┌──────────▼──────────────┐
                    │  Branch (Quality OK?)   │ ◄─── PASS/FAIL
                    └──┬──────────────────┬──┘
                       │                  │
            ┌──────────▼──────────┐    ┌──▼──────────────┐
            │ Transform & Clean   │    │  Alert Email    │
            └──────────┬──────────┘    └─────────────────┘
                       │
            ┌──────────▼──────────────┐
            │  Create Golden Records  │
            └──────────┬──────────────┘
                       │
            ┌──────────▼────────────────┐
            │ Engineer ML Features      │
            └──────────┬────────────────┘
                       │
            ┌──────────▼──────────────┐
            │ Enrich with References  │
            └──────────┬──────────────┘
                       │
            ┌──────────▼─────────────────┐
            │ Generate Summary Reports  │
            └──┬──────────────┬──────┬──┘
               │              │      │
        ┌──────▼──────┐ ┌────▼───┐ ┌▼──────────────┐
        │ Export GCS  │ │API Send │ │Quality Check  │
        └─────────────┘ └────────┘ └─┬──────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
            ┌───────▼────────┐ ┌─────▼─────┐ ┌──────▼───────┐
            │ Success Email  │ │Slack Alert│ │Log Metadata  │
            └────────────────┘ └───────────┘ └──────────────┘
```

---

## 18. Production Best Practices

### Secrets Management
```python
from airflow.models import Variable
from airflow.exceptions import AirflowException

# DON'T: Hardcode credentials
# password = 'my_secret_password'  ❌

# DO: Use Airflow Secrets Backend
gcp_project = Variable.get('gcp_project_id')  # Set in UI
api_key = Variable.get('api_key')  # Set via Secrets Manager

# Or use connections
from airflow.models import Connection
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('bigquery_default')
# Connection configured in Airflow UI with credentials
```

### Idempotency (Safe to Re-run)
```python
# Use CREATE OR REPLACE (idempotent)
idempotent_query = """
CREATE OR REPLACE TABLE `project.warehouse.customers` AS
SELECT * FROM `project.staging.raw_customers`
WHERE customer_id IS NOT NULL
"""

# Avoid INSERT INTO (not idempotent - duplicates on retry)
# Instead use MERGE or WRITE_TRUNCATE
```

### Resource Management
```python
# Limit concurrent tasks
dag = DAG(
    dag_id='resource_aware_pipeline',
    default_args={
        **default_args,
        'pool': 'bigquery_pool',  # Limit to 5 concurrent BQ jobs
    },
    dag=dag
)

# Set task priority
high_priority = BigQueryInsertJobOperator(
    task_id='high_priority_query',
    configuration={'query': {'query': 'SELECT 1'}},
    location='US',
    priority_weight=100,  # Higher number = runs first
    dag=dag
)

low_priority = PythonOperator(
    task_id='low_priority_cleanup',
    python_callable=cleanup,
    priority_weight=10,
    dag=dag
)
```

### Monitoring & Observability
```python
# Enable detailed logging
import logging

logger = logging.getLogger(__name__)

def observable_task(**context):
    logger.info(f"Starting task: {context['task'].task_id}")
    logger.info(f"Execution date: {context['execution_date']}")
    
    # Log metrics for monitoring
    metrics = {
        'task_id': context['task'].task_id,
        'duration_seconds': 120,
        'rows_processed': 1000,
        'status': 'SUCCESS'
    }
    
    logger.info(f"Metrics: {metrics}")
    
    # Push to XCom for alerting
    context['task_instance'].xcom_push(
        key='task_metrics',
        value=metrics
    )

observable = PythonOperator(
    task_id='observable_task',
    python_callable=observable_task,
    provide_context=True,
    dag=dag
)
```

---

## 19. Key Configuration Parameters

```python
# In dags_folder/config.yaml or dags/config.py

CONFIG = {
    # Execution settings
    'schedule': '0 2 * * *',  # 2 AM daily
    'timezone': 'UTC',
    'max_active_runs': 1,
    'catchup': False,
    
    # Data parameters
    'source_bucket': 'raw-data-bucket',
    'staging_dataset': 'staging',
    'warehouse_dataset': 'warehouse',
    'export_bucket': 'analytics-bucket',
    
    # BigQuery settings
    'location': 'US',
    'max_bytes_billed': 1000000000,  # $5 limit
    
    # Alerts
    'alert_email': 'data-team@company.com',
    'slack_webhook': 'https://hooks.slack.com/...',
    
    # Retries
    'retries': 2,
    'retry_delay_minutes': 5,
}

# Use in DAG
dag = DAG(
    dag_id='configurable_pipeline',
    schedule_interval=CONFIG['schedule'],
    default_args={'owner': 'data-engineering'},
)
```

---

## 20. Troubleshooting Common Issues

### Issue 1: Task Timeout
```python
# Solution: Increase execution timeout
slow_query = BigQueryInsertJobOperator(
    task_id='slow_aggregation',
    configuration={'query': {'query': 'expensive query'}},
    location='US',
    execution_timeout=timedelta(hours=3),  # Increased from 1 hour
    dag=dag
)
```

### Issue 2: XCom Pull Returns None
```python
# Solution: Check task runs before pulling
def safe_xcom_pull(**context):
    try:
        value = context['task_instance'].xcom_pull(
            task_ids='upstream_task',
            key='my_key'
        )
        if value is None:
            raise ValueError("Upstream task didn't set XCom value")
        return value
    except Exception as e:
        logger.error(f"XCom pull failed: {e}")
        raise
```

### Issue 3: BigQuery Job Fails Silently
```python
# Solution: Add error callback
def on_bq_failure(context):
    error = context['exception']
    logger.error(f"BigQuery job failed: {error}")
    # Send alert
    send_slack_alert(f"Pipeline failed: {error}")

failed_query = BigQueryInsertJobOperator(
    task_id='error_prone_query',
    configuration={'query': {'query': 'SELECT *'}},
    location='US',
    on_failure_callback=on_bq_failure,
    dag=dag
)
```

---

## Summary: DAG Execution Flow

1. **Ingestion**: Extract data from GCS → Load to BigQuery staging
2. **Validation**: Quality checks, duplicate detection, schema validation
3. **Transformation**: Data cleaning, type conversion, deduplication
4. **Enrichment**: Join with reference data, feature engineering
5. **Aggregation**: Create reports, summaries, metrics
6. **Export**: Send to GCS, APIs, or external systems
7. **Monitoring**: Log metadata, send alerts, track SLAs
8. **Cleanup**: Archive old data, update watermarks

**Total Pipeline**: Raw data → Clean warehouse → Analytics-ready features → Reports