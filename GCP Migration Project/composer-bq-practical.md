# Cloud Composer & BigQuery - Practical Hands-On Guide

## Table of Contents
1. [Step-by-Step Setup from Scratch](#1-step-by-step-setup-from-scratch)
2. [Your First DAG - Hello World](#2-your-first-dag---hello-world)
3. [Real-World Scenarios with Complete Code](#3-real-world-scenarios-with-complete-code)
4. [Debugging & Testing Guide](#4-debugging--testing-guide)
5. [Production Deployment Checklist](#5-production-deployment-checklist)

---

## 1. Step-by-Step Setup from Scratch

### Day 1: Initial GCP Setup (30 minutes)

**Step 1: Create GCP Project**

```bash
# Login to Google Cloud
gcloud auth login

# Create a new project
gcloud projects create my-de-project-2024 \
    --name="Data Engineering Project"

# Set as default
gcloud config set project my-de-project-2024

# Enable billing (required for Composer)
# Go to: https://console.cloud.google.com/billing
# Link your billing account to the project
```

**Step 2: Enable Required APIs**

```bash
# Enable all APIs at once
gcloud services enable \
    composer.googleapis.com \
    bigquery.googleapis.com \
    storage-api.googleapis.com \
    cloudscheduler.googleapis.com \
    cloudresourcemanager.googleapis.com

# Verify APIs are enabled
gcloud services list --enabled | grep -E 'composer|bigquery|storage'
```

**Step 3: Create Service Account**

```bash
# Create service account for local development
gcloud iam service-accounts create composer-dev-sa \
    --display-name="Composer Development Service Account"

# Get service account email
SA_EMAIL=$(gcloud iam service-accounts list \
    --filter="displayName:'Composer Development Service Account'" \
    --format='value(email)')

echo "Service Account: $SA_EMAIL"

# Grant necessary roles
gcloud projects add-iam-policy-binding my-de-project-2024 \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/composer.worker"

gcloud projects add-iam-policy-binding my-de-project-2024 \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding my-de-project-2024 \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.admin"

# Download key
gcloud iam service-accounts keys create ~/composer-dev-key.json \
    --iam-account=$SA_EMAIL

# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/composer-dev-key.json"

# Add to ~/.bashrc or ~/.zshrc for persistence
echo 'export GOOGLE_APPLICATION_CREDENTIALS="$HOME/composer-dev-key.json"' >> ~/.bashrc
```

**Step 4: Create Cloud Composer Environment**

```bash
# Create Composer environment (takes 20-30 minutes)
gcloud composer environments create my-first-composer \
    --location us-central1 \
    --zone us-central1-a \
    --machine-type n1-standard-4 \
    --node-count 3 \
    --disk-size 50 \
    --python-version 3 \
    --image-version composer-2.7.3-airflow-2.7.3 \
    --service-account $SA_EMAIL

# Monitor creation progress
gcloud composer environments describe my-first-composer \
    --location us-central1 \
    --format="value(state)"

# Get Airflow web UI URL
AIRFLOW_URL=$(gcloud composer environments describe my-first-composer \
    --location us-central1 \
    --format="value(config.airflowUri)")

echo "Airflow UI: $AIRFLOW_URL"

# Get DAGs bucket location
DAGS_BUCKET=$(gcloud composer environments describe my-first-composer \
    --location us-central1 \
    --format="value(config.dagGcsPrefix)")

echo "DAGs Bucket: $DAGS_BUCKET"
```

**Step 5: Setup Local Development Environment**

```bash
# Create project directory
mkdir -p ~/composer-projects/my-first-project
cd ~/composer-projects/my-first-project

# Create project structure
mkdir -p dags/{utils,sql} plugins tests config data

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Create requirements.txt
cat > requirements.txt << 'EOF'
# Airflow (match Composer version)
apache-airflow==2.7.3
apache-airflow-providers-google==10.10.1

# BigQuery
google-cloud-bigquery==3.14.0
google-cloud-bigquery-storage==2.22.0
db-dtypes==1.2.0

# GCS
google-cloud-storage==2.14.0

# Data Processing
pandas==2.1.4
numpy==1.26.2
pyarrow==14.0.2

# Utilities
python-dotenv==1.0.0
pyyaml==6.0.1

# Testing
pytest==7.4.3
pytest-mock==3.12.0

# Code Quality
black==23.12.1
flake8==7.0.0
EOF

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file for local development
cat > .env << EOF
PROJECT_ID=my-de-project-2024
DATASET_ID=analytics
GCS_BUCKET=my-de-project-2024-data
GOOGLE_APPLICATION_CREDENTIALS=$HOME/composer-dev-key.json
EOF

# Create .gitignore
cat > .gitignore << 'EOF'
venv/
__pycache__/
*.pyc
.env
*.json
.DS_Store
.idea/
.vscode/
*.log
EOF

# Initialize git
git init
git add .
git commit -m "Initial project setup"
```

**Step 6: Create BigQuery Dataset**

```bash
# Create dataset
bq mk \
    --dataset \
    --location=US \
    --description="Analytics dataset" \
    my-de-project-2024:analytics

# Verify
bq ls my-de-project-2024:

# Create GCS bucket for data
gsutil mb -l us-central1 -c STANDARD gs://my-de-project-2024-data

# Verify
gsutil ls
```

---

## 2. Your First DAG - Hello World

### Understanding the Basics

**What is a DAG?**
Think of a DAG as a recipe:
- **Ingredients** = Your data
- **Steps** = Tasks (queries, transformations)
- **Order** = Dependencies (Task A before Task B)
- **When to cook** = Schedule (daily, hourly)

### Step 1: Create Your First DAG File

Create `dags/hello_world_dag.py`:

```python
"""
My First Airflow DAG - Hello World
This DAG demonstrates the basics of Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ============================================
# STEP 1: Define Default Arguments
# ============================================
# These apply to all tasks in the DAG

default_args = {
    'owner': 'john-doe',  # Your name
    'depends_on_past': False,  # Each run is independent
    'email': ['john@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,  # Try 2 more times if task fails
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2024, 1, 1),  # When DAG becomes active
}

# ============================================
# STEP 2: Create the DAG
# ============================================

dag = DAG(
    dag_id='hello_world_dag',  # Unique identifier
    default_args=default_args,
    description='My first Airflow DAG',
    schedule_interval='@daily',  # Run once per day
    catchup=False,  # Don't run for past dates
    tags=['tutorial', 'beginner'],
)

# ============================================
# STEP 3: Define Tasks
# ============================================

# Task 1: Print hello message
def print_hello():
    """Simple Python function"""
    print("Hello from Airflow!")
    print("This is my first DAG")
    return "Hello message printed"

task_hello = PythonOperator(
    task_id='print_hello',  # Unique task name
    python_callable=print_hello,  # Function to execute
    dag=dag,
)

# Task 2: Get execution date
def print_execution_date(**context):
    """Access Airflow context variables"""
    execution_date = context['ds']  # YYYY-MM-DD format
    dag_run_id = context['run_id']
    
    print(f"Execution Date: {execution_date}")
    print(f"DAG Run ID: {dag_run_id}")
    
    # Push data to next task (XCom)
    context['task_instance'].xcom_push(
        key='execution_info',
        value={'date': execution_date, 'run_id': dag_run_id}
    )
    
    return execution_date

task_date = PythonOperator(
    task_id='print_execution_date',
    python_callable=print_execution_date,
    provide_context=True,  # Pass Airflow context
    dag=dag,
)

# Task 3: Run bash command
task_bash = BashOperator(
    task_id='print_date_bash',
    bash_command='date',
    dag=dag,
)

# Task 4: Query BigQuery
task_bigquery = BigQueryInsertJobOperator(
    task_id='query_bigquery',
    configuration={
        'query': {
            'query': """
                SELECT 
                    '{{ ds }}' as execution_date,
                    CURRENT_TIMESTAMP() as query_time,
                    'Hello from BigQuery!' as message
            """,
            'useLegacySql': False,
        }
    },
    dag=dag,
)

# Task 5: Process previous task's output
def process_xcom_data(**context):
    """Pull data from previous task"""
    ti = context['task_instance']
    
    # Pull data from previous task
    execution_info = ti.xcom_pull(
        task_ids='print_execution_date',
        key='execution_info'
    )
    
    print(f"Received from previous task: {execution_info}")
    print(f"Processing data for date: {execution_info['date']}")

task_process = PythonOperator(
    task_id='process_data',
    python_callable=process_xcom_data,
    provide_context=True,
    dag=dag,
)

# ============================================
# STEP 4: Define Task Dependencies
# ============================================

# Linear flow: task_hello → task_date → task_bash → task_bigquery
task_hello >> task_date >> task_bash >> task_bigquery

# Parallel branch: task_date also feeds into task_process
task_date >> task_process
```

### Step 2: Test DAG Locally

```bash
# Test if DAG file has valid Python syntax
python dags/hello_world_dag.py

# List all DAGs (should show hello_world_dag)
airflow dags list

# Check for import errors
python -c "from airflow.models import DagBag; db = DagBag(); print(db.import_errors)"

# Test individual task
airflow tasks test hello_world_dag print_hello 2024-01-15

# Test entire DAG
airflow dags test hello_world_dag 2024-01-15
```

### Step 3: Deploy to Cloud Composer

```bash
# Get DAGs folder
DAGS_FOLDER=$(gcloud composer environments describe my-first-composer \
    --location us-central1 \
    --format="value(config.dagGcsPrefix)")

# Upload DAG
gsutil cp dags/hello_world_dag.py $DAGS_FOLDER

# Verify upload
gsutil ls $DAGS_FOLDER

# Wait 1-2 minutes for Composer to detect the DAG

# Check if DAG appears in Airflow UI
# Open Airflow UI URL from earlier
# You should see "hello_world_dag" in the list
```

### Step 4: Run and Monitor

**Via Airflow UI:**
1. Navigate to Airflow UI
2. Find `hello_world_dag`
3. Toggle the switch to **ON** (unpause)
4. Click **Trigger DAG** button
5. Click on DAG name to see graph view
6. Watch tasks turn green as they complete

**Via Command Line:**

```bash
# Trigger DAG
gcloud composer environments run my-first-composer \
    --location us-central1 \
    dags trigger -- hello_world_dag

# Check DAG status
gcloud composer environments run my-first-composer \
    --location us-central1 \
    dags list-runs -- -d hello_world_dag

# View logs
gcloud composer environments run my-first-composer \
    --location us-central1 \
    tasks logs -- hello_world_dag print_hello 2024-01-15
```

---

## 3. Real-World Scenarios with Complete Code

### Scenario 1: CSV to BigQuery Daily Load

**Use Case:** Every day at 2 AM, load yesterday's sales data from CSV files in GCS to BigQuery.

**File:** `dags/csv_to_bigquery_dag.py`

```python
"""
Daily CSV to BigQuery ETL Pipeline
Loads sales data from GCS to BigQuery
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'csv_to_bigquery_daily',
    default_args=default_args,
    description='Load CSV files to BigQuery daily',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'sales', 'production'],
)

# Step 1: Check if source file exists
check_file = GCSObjectExistenceSensor(
    task_id='check_csv_file',
    bucket='my-de-project-2024-data',
    object='sales/{{ ds }}/sales_data.csv',  # Template: sales/2024-01-15/sales_data.csv
    timeout=600,  # Wait up to 10 minutes
    poke_interval=60,  # Check every minute
    mode='poke',
    dag=dag,
)

# Step 2: Load CSV to BigQuery (raw staging table)
load_csv = GCSToBigQueryOperator(
    task_id='load_csv_to_staging',
    bucket='my-de-project-2024-data',
    source_objects=['sales/{{ ds }}/sales_data.csv'],
    destination_project_dataset_table='my-de-project-2024.analytics.sales_staging',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',  # Replace data
    schema_fields=[
        {'name': 'sale_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'product_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'sale_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    ],
    dag=dag,
)

# Step 3: Data quality check
check_data = BigQueryCheckOperator(
    task_id='check_loaded_data',
    sql="""
        SELECT 
            COUNT(*) > 0 AND
            COUNTIF(amount < 0) = 0 AND
            COUNTIF(quantity < 0) = 0
        FROM `my-de-project-2024.analytics.sales_staging`
    """,
    use_legacy_sql=False,
    dag=dag,
)

# Step 4: Transform and load to production table
transform_load = BigQueryInsertJobOperator(
    task_id='transform_and_load',
    configuration={
        'query': {
            'query': """
                INSERT INTO `my-de-project-2024.analytics.sales`
                SELECT 
                    sale_id,
                    customer_id,
                    product_id,
                    quantity,
                    amount,
                    sale_date,
                    amount * quantity as total_value,
                    CURRENT_TIMESTAMP() as loaded_at
                FROM `my-de-project-2024.analytics.sales_staging`
                WHERE sale_date = DATE('{{ ds }}')
            """,
            'useLegacySql': False,
        }
    },
    dag=dag,
)

# Step 5: Update summary table
update_summary = BigQueryInsertJobOperator(
    task_id='update_daily_summary',
    configuration={
        'query': {
            'query': """
                MERGE INTO `my-de-project-2024.analytics.daily_sales_summary` target
                USING (
                    SELECT 
                        sale_date,
                        COUNT(*) as total_sales,
                        SUM(amount * quantity) as total_revenue,
                        COUNT(DISTINCT customer_id) as unique_customers
                    FROM `my-de-project-2024.analytics.sales`
                    WHERE sale_date = DATE('{{ ds }}')
                    GROUP BY sale_date
                ) source
                ON target.sale_date = source.sale_date
                WHEN MATCHED THEN
                    UPDATE SET
                        total_sales = source.total_sales,
                        total_revenue = source.total_revenue,
                        unique_customers = source.unique_customers,
                        updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (sale_date, total_sales, total_revenue, unique_customers, created_at)
                    VALUES (source.sale_date, source.total_sales, source.total_revenue, 
                            source.unique_customers, CURRENT_TIMESTAMP())
            """,
            'useLegacySql': False,
        }
    },
    dag=dag,
)

# Define workflow
check_file >> load_csv >> check_data >> transform_load >> update_summary
```

**Setup Tables:**

```sql
-- Create staging table
CREATE OR REPLACE TABLE `my-de-project-2024.analytics.sales_staging` (
    sale_id STRING,
    customer_id INT64,
    product_id STRING,
    quantity INT64,
    amount FLOAT64,
    sale_date DATE
);

-- Create production table (partitioned)
CREATE OR REPLACE TABLE `my-de-project-2024.analytics.sales` (
    sale_id STRING,
    customer_id INT64,
    product_id STRING,
    quantity INT64,
    amount FLOAT64,
    sale_date DATE,
    total_value FLOAT64,
    loaded_at TIMESTAMP
)
PARTITION BY sale_date
CLUSTER BY customer_id;

-- Create summary table
CREATE OR REPLACE TABLE `my-de-project-2024.analytics.daily_sales_summary` (
    sale_date DATE,
    total_sales INT64,
    total_revenue FLOAT64,
    unique_customers INT64,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

**Upload Sample Data:**

```bash
# Create sample CSV
cat > /tmp/sales_data.csv << 'EOF'
sale_id,customer_id,product_id,quantity,amount,sale_date
S001,1001,P101,2,50.00,2024-01-15
S002,1002,P102,1,75.50,2024-01-15
S003,1001,P103,3,25.00,2024-01-15
EOF

# Upload to GCS
gsutil cp /tmp/sales_data.csv gs://my-de-project-2024-data/sales/2024-01-15/
```

**Deploy and Run:**

```bash
# Upload DAG
gsutil cp dags/csv_to_bigquery_dag.py gs://[DAGS_BUCKET]/

# Trigger manually for testing
gcloud composer environments run my-first-composer \
    --location us-central1 \
    dags trigger -- csv_to_bigquery_daily \
    --conf '{"execution_date":"2024-01-15"}'
```

### Scenario 2: API Data Ingestion Pipeline

**Use Case:** Fetch data from REST API hourly, process it, and store in BigQuery.

**File:** `dags/api_to_bigquery_dag.py`

```python
"""
Hourly API Data Ingestion Pipeline
Fetches data from external API and loads to BigQuery
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'api-team',
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'api_to_bigquery_hourly',
    default_args=default_args,
    description='Fetch API data and load to BigQuery',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False,
    tags=['api', 'ingestion'],
)

def fetch_api_data(**context):
    """
    Fetch data from API
    Returns: Number of records fetched
    """
    execution_date = context['ds']
    execution_hour = context['execution_date'].hour
    
    # API endpoint
    api_url = "https://api.example.com/v1/events"
    
    # API parameters
    params = {
        'date': execution_date,
        'hour': execution_hour,
        'limit': 1000
    }
    
    # Add authentication if needed
    headers = {
        'Authorization': 'Bearer YOUR_API_TOKEN',
        'Content-Type': 'application/json'
    }
    
    try:
        logger.info(f"Fetching data for {execution_date} hour {execution_hour}")
        
        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        records = data.get('events', [])
        
        logger.info(f"Fetched {len(records)} records")
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Basic cleaning
        df['fetched_at'] = datetime.now()
        df['execution_date'] = execution_date
        
        # Load to BigQuery
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        
        bq_hook.insert_all(
            project_id='my-de-project-2024',
            dataset_id='analytics',
            table_id='api_events_raw',
            rows=df.to_dict('records'),
        )
        
        logger.info(f"Loaded {len(df)} records to BigQuery")
        
        # Push count to XCom for next task
        context['task_instance'].xcom_push(key='record_count', value=len(df))
        
        return len(df)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing API data: {e}")
        raise

fetch_data_task = PythonOperator(
    task_id='fetch_api_data',
    python_callable=fetch_api_data,
    provide_context=True,
    dag=dag,
)

# Process and deduplicate
process_task = BigQueryInsertJobOperator(
    task_id='process_api_data',
    configuration={
        'query': {
            'query': """
                INSERT INTO `my-de-project-2024.analytics.api_events_processed`
                SELECT 
                    event_id,
                    event_type,
                    user_id,
                    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', event_timestamp) as event_timestamp,
                    properties,
                    execution_date,
                    fetched_at
                FROM (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY event_id 
                            ORDER BY fetched_at DESC
                        ) as row_num
                    FROM `my-de-project-2024.analytics.api_events_raw`
                    WHERE execution_date = '{{ ds }}'
                )
                WHERE row_num = 1
            """,
            'useLegacySql': False,
        }
    },
    dag=dag,
)

def send_summary(**context):
    """Send summary of data loaded"""
    record_count = context['task_instance'].xcom_pull(
        task_ids='fetch_api_data',
        key='record_count'
    )
    
    logger.info(f"Summary: Loaded {record_count} records for {context['ds']}")
    # Here you could send Slack/email notification

summary_task = PythonOperator(
    task_id='send_summary',
    python_callable=send_summary,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> process_task >> summary_task
```

### Scenario 3: Multi-Source Data Integration

**Use Case:** Combine data from MySQL database, CSV files, and BigQuery tables into a unified analytics table.

**File:** `dags/multi_source_integration_dag.py`

```python
"""
Multi-Source Data Integration Pipeline
Combines data from MySQL, CSV, and BigQuery
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'integration-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'multi_source_integration',
    default_args=default_args,
    description='Integrate data from multiple sources',
    schedule_interval='0 3 * * *',  # 3 AM daily
    catchup=False,
    tags=['integration', 'multi-source'],
)

def extract_mysql_data(**context):
    """Extract customer data from MySQL"""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_prod')
    
    sql = """
        SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            registration_date,
            country
        FROM customers
        WHERE DATE(registration_date) <= %s
    """
    
    execution_date = context['ds']
    df = mysql_hook.get_pandas_df(sql, parameters=[execution_date])
    
    logger.info(f"Extracted {len(df)} customers from MySQL")
    
    # Load to BigQuery staging
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    
    bq_hook.insert_all(
        project_id='my-de-project-2024',
        dataset_id='staging',
        table_id='mysql_customers',
        rows=df.to_dict('records'),
    )
    
    return len(df)

extract_mysql = PythonOperator(
    task_id='extract_mysql_data',
    python_callable=extract_mysql_data,
    provide_context=True,
    dag=dag,
)

# Load CSV data (orders)
load_csv_orders = GCSToBigQueryOperator(
    task_id='load_csv_orders',
    bucket='my-de-project-2024-data',
    source_objects=['orders/{{ ds }}/*.csv'],
    destination_project_dataset_table='my-de-project-2024.staging.csv_orders',
    source_format='CSV',
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    dag=dag,
)

# Integrate all sources
integrate_data = BigQueryInsertJobOperator(
    task_id='integrate_all_sources',
    configuration={
        'query': {
            'query': """
                -- Create integrated customer analytics table
                CREATE OR REPLACE TABLE `my-de-project-2024.analytics.customer_360` AS
                
                WITH customer_base AS (
                    -- From MySQL
                    SELECT 
                        customer_id,
                        CONCAT(first_name, ' ', last_name) as customer_name,
                        email,
                        registration_date,
                        country
                    FROM `my-de-project-2024.staging.mysql_customers`
                ),
                
                order_summary AS (
                    -- From CSV files
                    SELECT 
                        customer_id,
                        COUNT(*) as total_orders,
                        SUM(order_amount) as total_spent,
                        MAX(order_date) as last_order_date
                    FROM `my-de-project-2024.staging.csv_orders`
                    WHERE order_date <= DATE('{{ ds }}')
                    GROUP BY customer_id
                ),
                
                support_tickets AS (
                    -- From existing BigQuery table
                    SELECT 
                        customer_id,
                        COUNT(*) as ticket_count,
                        AVG(satisfaction_score) as avg_satisfaction
                    FROM `my-de-project-2024.analytics.support_tickets`
                    WHERE ticket_date <= DATE('{{ ds }}')
                    GROUP BY customer_id
                )
                
                -- Combine all sources
                SELECT 
                    c.customer_id,
                    c.customer_name,
                    c.email,
                    c.registration_date,
                    c.country,
                    COALESCE(o.total_orders, 0) as total_orders,
                    COALESCE(o.total_spent, 0) as lifetime_value,
                    o.last_order_date,
                    COALESCE(s.ticket_count, 0) as support_tickets,
                    COALESCE(s.avg_satisfaction, 0) as avg_satisfaction,
                    -- Calculate customer segment
                    CASE 
                        WHEN COALESCE(o.total_spent, 0) > 10000 THEN 'VIP'
                        WHEN COALESCE(o.total_spent, 0) > 5000 THEN 'Premium'
                        WHEN COALESCE(o.total_spent, 0) > 1000 THEN 'Regular'
                        ELSE 'New'
                    END as customer_segment,
                    DATE('{{ ds }}') as snapshot_date
                FROM customer_base c
                LEFT JOIN order_summary o USING(customer_id)
                LEFT JOIN support_tickets s USING(customer_id)
            """,
            'useLegacySql': False,
        }
    },
    dag=dag,
)

# Run in parallel then integrate
[extract_mysql, load_csv_orders] >> integrate_data
```

---

## 4. Debugging & Testing Guide

### Testing DAGs Locally

**Create:** `tests/test_dags.py`

```python
"""
Unit tests for Airflow DAGs
"""

import pytest
from airflow.models import DagBag
from datetime import datetime

class TestDagIntegrity:
    """Test DAG integrity and structure"""
    
    def setup_method(self):
        """Load all DAGs"""
        self.dagbag = DagBag(dag_folder='dags/', include_examples=False)
    
    def test_no_import_errors(self):
        """Ensure no DAG has import errors"""
        assert len(self.dagbag.import_errors) == 0, \
            f"DAG import errors: {self.dagbag.import_errors}"
    
    def test_dag_count(self):
        """Ensure we have DAGs loaded"""
        assert len(self.dagbag.dags) > 0, "No DAGs found"
    
    def test_dag_tags(self):
        """Ensure all DAGs have tags"""
        for dag_id, dag in self.dagbag.dags.items():
            assert len(dag.tags) > 0, f"DAG {dag_id} has no tags"
    
    def test_dag_owners(self):
        """Ensure all DAGs have owners"""
        for dag_id, dag in self.dagbag.dags.items():
            assert dag.default_args.get('owner'), \
                f"DAG {dag_id} has no owner"
    
    def test_dag_retries(self):
        """Ensure all DAGs have retry logic"""
        for dag_id, dag in self.dagbag.dags.items():
            assert dag.default_args.get('retries', 0) > 0, \
                f"DAG {dag_id} has no retry logic"

class TestSpecificDags:
    """Test specific DAG logic"""
    
    def setup_method(self):
        self.dagbag = DagBag(dag_folder='dags/', include_examples=False)
    
    def test_hello_world_dag_structure(self):
        """Test hello_world_dag has correct structure"""
        dag_id = 'hello_world_dag'
        dag = self.dagbag.get_dag(dag_id)
        
        assert dag is not None, f"DAG {dag_id} not found"
        
        # Check task count
        assert len(dag.tasks) == 5, "Expected 5 tasks"
        
        # Check specific tasks exist
        task_ids = [task.task_id for task in dag.tasks]
        expected_tasks = [
            'print_hello',
            'print_execution_date',
            'print_date_bash',
            'query_bigquery',
            'process_data'
        ]
        
        for expected_task in expected_tasks:
            assert expected_task in task_ids, \
                f"Task {expected_task} not found"
    
    def test_dag_dependencies(self):
        """Test task dependencies are correct"""
        dag = self.dagbag.get_dag('hello_world_dag')
        
        # Get task by ID
        print_hello = dag.get_task('print_hello')
        print_date = dag.get_task('print_execution_date')
        
        # Check dependencies
        downstream = print_hello.get_direct_relatives(upstream=False)
        assert print_date in downstream, \
            "print_execution_date should be downstream of print_hello"

# Run tests
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
```

**Run Tests:**

```bash
# Install pytest if not already
pip install pytest

# Run all tests
pytest tests/test_dags.py -v

# Run specific test
pytest tests/test_dags.py::TestDagIntegrity::test_no_import_errors -v

# Run with coverage
pip install pytest-cov
pytest tests/ --cov=dags --cov-report=html
```

### Common Debugging Techniques

**1. Check DAG Parse Errors:**

```python
# Create: dags/check_dags.py
from airflow.models import DagBag
import sys

dagbag = DagBag(dag_folder='dags/')

if dagbag.import_errors:
    print("❌ DAG Import Errors Found:")
    for filename, error in dagbag.import_errors.items():
        print(f"\nFile: {filename}")
        print(f"Error: {error}")
    sys.exit(1)
else:
    print("✅ All DAGs loaded successfully")
    print(f"Total DAGs: {len(dagbag.dags)}")
    for dag_id in dagbag.dags:
        print(f"  - {dag_id}")
```

**2. Validate SQL Queries:**

```python
# Create: dags/utils/sql_validator.py
from google.cloud import bigquery

def validate_query(query: str, project_id: str) -> dict:
    """
    Validate BigQuery SQL without executing
    
    Returns:
        dict with validation results
    """
    client = bigquery.Client(project=project_id)
    
    job_config = bigquery.QueryJobConfig(dry_run=True)
    
    try:
        query_job = client.query(query, job_config=job_config)
        
        return {
            'valid': True,
            'bytes_processed': query_job.total_bytes_processed,
            'estimated_cost': query_job.total_bytes_processed / 1e12 * 5  # $5 per TB
        }
    except Exception as e:
        return {
            'valid': False,
            'error': str(e)
        }

# Usage in DAG
def validate_transform_query(**context):
    """Validate query before running"""
    query = """
        SELECT * FROM `project.dataset.table`
        WHERE date = '{{ ds }}'
    """
    
    # Replace template variables
    query = query.replace('{{ ds }}', context['ds'])
    
    result = validate_query(query, 'my-de-project-2024')
    
    if not result['valid']:
        raise ValueError(f"Invalid query: {result['error']}")
    
    print(f"Query valid. Will process {result['bytes_processed']} bytes")
    print(f"Estimated cost: ${result['estimated_cost']:.4f}")
```

**3. Debug XCom Issues:**

```python
def debug_xcom(**context):
    """Debug XCom data passing"""
    ti = context['task_instance']
    
    # Get all XCom values for this DAG run
    all_xcoms = ti.xcom_pull(task_ids=None, key=None)
    
    print("All XCom values:")
    for task_id, values in all_xcoms.items():
        print(f"\nTask: {task_id}")
        print(f"Values: {values}")
    
    # Get specific XCom
    specific_value = ti.xcom_pull(task_ids='previous_task', key='my_key')
    print(f"\nSpecific value: {specific_value}")
    
    # Check if XCom exists
    if specific_value is None:
        print("⚠️ Warning: XCom value not found!")
        print("Possible issues:")
        print("1. Task 'previous_task' hasn't run yet")
        print("2. Key 'my_key' doesn't exist")
        print("3. XCom was not pushed from previous task")
```

**4. Test Task in Isolation:**

```bash
# Test single task without dependencies
airflow tasks test my_dag_id my_task_id 2024-01-15

# Clear task state and re-run
airflow tasks clear my_dag_id --task-regex my_task_id

# Run task with verbose logging
airflow tasks test my_dag_id my_task_id 2024-01-15 --verbose
```

**5. Monitor Task Performance:**

```python
# Create: dags/utils/performance_monitor.py
import time
import functools
import logging

logger = logging.getLogger(__name__)

def monitor_performance(func):
    """Decorator to monitor function performance"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_memory = get_memory_usage()
        
        logger.info(f"Starting {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            
            end_time = time.time()
            end_memory = get_memory_usage()
            
            duration = end_time - start_time
            memory_used = end_memory - start_memory
            
            logger.info(f"Completed {func.__name__}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Memory used: {memory_used:.2f} MB")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed {func.__name__}: {e}")
            raise
    
    return wrapper

def get_memory_usage():
    """Get current memory usage in MB"""
    import psutil
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

# Usage
@monitor_performance
def process_large_dataset(**context):
    """Process large dataset with monitoring"""
    # Your processing logic
    pass
```

---

## 5. Production Deployment Checklist

### Pre-Deployment Checklist

**Create:** `deployment_checklist.md`

```markdown
# Production Deployment Checklist

## 1. Code Quality
- [ ] All DAGs pass `pytest` tests
- [ ] No import errors (`python dags/check_dags.py`)
- [ ] Code formatted with `black`
- [ ] Linting passes (`flake8 dags/`)
- [ ] No hardcoded credentials
- [ ] All secrets in Secret Manager or Variables

## 2. DAG Configuration
- [ ] Appropriate `start_date` set
- [ ] `catchup=False` for new DAGs
- [ ] Retry logic configured (min 2 retries)
- [ ] Email alerts configured
- [ ] Timeout set for long-running tasks
- [ ] `max_active_runs` set appropriately
- [ ] Meaningful tags added

## 3. BigQuery Optimization
- [ ] Tables are partitioned
- [ ] Clustering fields defined
- [ ] Queries use partition filters
- [ ] Cost estimates reviewed (`dry_run`)
- [ ] Appropriate write disposition set
- [ ] No `SELECT *` in production queries

## 4. Error Handling
- [ ] Try-catch blocks in Python functions
- [ ] Graceful failure handling
- [ ] Slack/email alerts on failure
- [ ] Logging statements added
- [ ] Rollback strategy documented

## 5. Monitoring
- [ ] Key metrics identified
- [ ] Data quality checks implemented
- [ ] Anomaly detection in place
- [ ] Dashboard created for monitoring
- [ ] On-call rotation defined

## 6. Documentation
- [ ] DAG purpose documented
- [ ] Task descriptions added
- [ ] Dependencies documented
- [ ] Runbook created
- [ ] Architecture diagram updated

## 7. Security
- [ ] Service account permissions minimal
- [ ] No PII in logs
- [ ] Data encrypted at rest
- [ ] API keys in Secret Manager
- [ ] Access controls reviewed

## 8. Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Tested in dev environment
- [ ] Backfill tested
- [ ] Performance tested with production data volume

## 9. Deployment Process
- [ ] Change request approved
- [ ] Deployment window scheduled
- [ ] Rollback plan ready
- [ ] Team notified
- [ ] Post-deployment verification steps defined
```

### Deployment Script

**Create:** `scripts/deploy.sh`

```bash
#!/bin/bash

# Deployment script for Cloud Composer DAGs

set -e  # Exit on error

# Configuration
PROJECT_ID="my-de-project-2024"
COMPOSER_ENV="my-first-composer"
LOCATION="us-central1"
DAGS_FOLDER="dags"

echo "========================================="
echo "  Deploying to Cloud Composer"
echo "========================================="
echo "Project: $PROJECT_ID"
echo "Environment: $COMPOSER_ENV"
echo "Location: $LOCATION"
echo ""

# Step 1: Validate Python syntax
echo "Step 1: Validating Python syntax..."
for file in $DAGS_FOLDER/*.py; do
    echo "  Checking $file"
    python -m py_compile "$file"
done
echo "✅ All files have valid syntax"
echo ""

# Step 2: Run tests
echo "Step 2: Running tests..."
pytest tests/ -v || {
    echo "❌ Tests failed! Aborting deployment."
    exit 1
}
echo "✅ All tests passed"
echo ""

# Step 3: Check for import errors
echo "Step 3: Checking for DAG import errors..."
python -c "
from airflow.models import DagBag
import sys

dagbag = DagBag(dag_folder='$DAGS_FOLDER/', include_examples=False)

if dagbag.import_errors:
    print('❌ Import errors found:')
    for filename, error in dagbag.import_errors.items():
        print(f'  {filename}: {error}')
    sys.exit(1)
else:
    print('✅ No import errors')
" || exit 1
echo ""

# Step 4: Get DAGs bucket
echo "Step 4: Getting DAGs bucket..."
DAGS_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
    --location $LOCATION \
    --format="value(config.dagGcsPrefix)" 2>/dev/null)

if [ -z "$DAGS_BUCKET" ]; then
    echo "❌ Failed to get DAGs bucket. Is the environment running?"
    exit 1
fi
echo "DAGs bucket: $DAGS_BUCKET"
echo ""

# Step 5: Backup existing DAGs
echo "Step 5: Backing up existing DAGs..."
BACKUP_DIR="backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p $BACKUP_DIR
gsutil -m cp -r $DAGS_BUCKET/* $BACKUP_DIR/ 2>/dev/null || true
echo "✅ Backup created: $BACKUP_DIR"
echo ""

# Step 6: Deploy DAGs
echo "Step 6: Deploying DAGs..."
gsutil -m cp -r $DAGS_FOLDER/* $DAGS_BUCKET/

# Copy utils folder if exists
if [ -d "$DAGS_FOLDER/utils" ]; then
    gsutil -m cp -r $DAGS_FOLDER/utils $DAGS_BUCKET/
fi

echo "✅ DAGs deployed successfully"
echo ""

# Step 7: Verify deployment
echo "Step 7: Verifying deployment..."
echo "Waiting 60 seconds for Composer to detect new DAGs..."
sleep 60

# List DAGs
echo "DAGs in Composer:"
gcloud composer environments run $COMPOSER_ENV \
    --location $LOCATION \
    dags list 2>/dev/null | tail -n +4 || {
    echo "⚠️ Could not verify DAG list"
}
echo ""

echo "========================================="
echo "  Deployment Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Check Airflow UI for new DAGs"
echo "2. Verify no import errors in UI"
echo "3. Trigger test run for critical DAGs"
echo "4. Monitor logs for any issues"
echo ""
echo "Airflow UI URL:"
gcloud composer environments describe $COMPOSER_ENV \
    --location $LOCATION \
    --format="value(config.airflowUri)"
```

**Make executable and run:**

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

### Environment Management

**Create:** `scripts/manage_environment.sh`

```bash
#!/bin/bash

# Manage Cloud Composer environments

PROJECT_ID="my-de-project-2024"
LOCATION="us-central1"

function create_environment() {
    ENV_NAME=$1
    ENV_TYPE=$2  # dev, staging, prod
    
    case $ENV_TYPE in
        dev)
            NODE_COUNT=3
            MACHINE_TYPE="n1-standard-2"
            ;;
        staging)
            NODE_COUNT=3
            MACHINE_TYPE="n1-standard-4"
            ;;
        prod)
            NODE_COUNT=5
            MACHINE_TYPE="n1-standard-8"
            ;;
        *)
            echo "Invalid environment type: $ENV_TYPE"
            exit 1
            ;;
    esac
    
    echo "Creating $ENV_TYPE environment: $ENV_NAME"
    
    gcloud composer environments create $ENV_NAME \
        --location $LOCATION \
        --node-count $NODE_COUNT \
        --machine-type $MACHINE_TYPE \
        --disk-size 50 \
        --python-version 3 \
        --image-version composer-2.7.3-airflow-2.7.3 \
        --env-variables "ENVIRONMENT=$ENV_TYPE" \
        --labels "environment=$ENV_TYPE,managed-by=terraform"
}

function update_packages() {
    ENV_NAME=$1
    
    echo "Updating PyPI packages for $ENV_NAME..."
    
    gcloud composer environments update $ENV_NAME \
        --location $LOCATION \
        --update-pypi-packages-from-file requirements.txt
}

function set_variables() {
    ENV_NAME=$1
    
    echo "Setting Airflow variables for $ENV_NAME..."
    
    gcloud composer environments run $ENV_NAME \
        --location $LOCATION \
        variables set -- project_id $PROJECT_ID
    
    gcloud composer environments run $ENV_NAME \
        --location $LOCATION \
        variables set -- environment $ENV_NAME
}

function scale_environment() {
    ENV_NAME=$1
    NODE_COUNT=$2
    
    echo "Scaling $ENV_NAME to $NODE_COUNT nodes..."
    
    gcloud composer environments update $ENV_NAME \
        --location $LOCATION \
        --node-count $NODE_COUNT
}

# Main menu
case ${1:-help} in
    create)
        create_environment $2 $3
        ;;
    update-packages)
        update_packages $2
        ;;
    set-variables)
        set_variables $2
        ;;
    scale)
        scale_environment $2 $3
        ;;
    *)
        echo "Usage: $0 {create|update-packages|set-variables|scale} [args]"
        echo ""
        echo "Commands:"
        echo "  create ENV_NAME ENV_TYPE    - Create new environment (dev/staging/prod)"
        echo "  update-packages ENV_NAME    - Update PyPI packages"
        echo "  set-variables ENV_NAME      - Set Airflow variables"
        echo "  scale ENV_NAME NODE_COUNT   - Scale environment"
        exit 1
        ;;
esac
```

### Monitoring Dashboard SQL

**Create:** `sql/monitoring_queries.sql`

```sql
-- Query 1: DAG Run Success Rate (Last 7 Days)
SELECT 
    dag_id,
    COUNT(*) as total_runs,
    COUNTIF(state = 'success') as successful_runs,
    COUNTIF(state = 'failed') as failed_runs,
    ROUND(COUNTIF(state = 'success') / COUNT(*) * 100, 2) as success_rate_pct,
    AVG(TIMESTAMP_DIFF(end_date, start_date, SECOND)) as avg_duration_sec
FROM `my-de-project-2024.composer_monitoring.dag_run`
WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY dag_id
ORDER BY success_rate_pct ASC;

-- Query 2: Slowest Tasks
SELECT 
    dag_id,
    task_id,
    AVG(TIMESTAMP_DIFF(end_date, start_date, SECOND)) as avg_duration_sec,
    MAX(TIMESTAMP_DIFF(end_date, start_date, SECOND)) as max_duration_sec,
    COUNT(*) as run_count
FROM `my-de-project-2024.composer_monitoring.task_instance`
WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND state = 'success'
GROUP BY dag_id, task_id
HAVING COUNT(*) >= 5
ORDER BY avg_duration_sec DESC
LIMIT 20;

-- Query 3: Task Failure Analysis
SELECT 
    dag_id,
    task_id,
    COUNT(*) as failure_count,
    ARRAY_AGG(STRUCT(execution_date, try_number) ORDER BY execution_date DESC LIMIT 5) as recent_failures
FROM `my-de-project-2024.composer_monitoring.task_instance`
WHERE state = 'failed'
    AND execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY dag_id, task_id
ORDER BY failure_count DESC
LIMIT 20;

-- Query 4: Data Freshness Check
SELECT 
    table_name,
    TIMESTAMP_MILLIS(creation_time) as created_at,
    TIMESTAMP_MILLIS(CAST(last_modified_time AS INT64)) as last_modified,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(CAST(last_modified_time AS INT64)), HOUR) as hours_since_update,
    row_count,
    ROUND(size_bytes / 1024 / 1024 / 1024, 2) as size_gb
FROM `my-de-project-2024.analytics.__TABLES__`
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(CAST(last_modified_time AS INT64)), HOUR) > 24
ORDER BY hours_since_update DESC;

-- Query 5: BigQuery Cost Analysis
SELECT 
    DATE(creation_time) as query_date,
    user_email,
    COUNT(*) as query_count,
    ROUND(SUM(total_bytes_processed) / 1024 / 1024 / 1024 / 1024, 2) as total_tb_processed,
    ROUND(SUM(total_bytes_processed) / 1024 / 1024 / 1024 / 1024 * 5, 2) as estimated_cost_usd
FROM `my-de-project-2024.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND state = 'DONE'
    AND job_type = 'QUERY'
GROUP BY query_date, user_email
ORDER BY query_date DESC, estimated_cost_usd DESC;
```

### Alerting Setup

**Create:** `dags/utils/alerting.py`

```python
"""
Alerting utilities for Airflow DAGs
"""

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.email import EmailOperator
import logging

logger = logging.getLogger(__name__)

def create_slack_alert(context, message_type='failure'):
    """
    Create Slack alert message
    
    Args:
        context: Airflow context
        message_type: 'failure', 'success', or 'warning'
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['ds']
    log_url = context['task_instance'].log_url
    
    # Emoji based on message type
    emoji_map = {
        'failure': '❌',
        'success': '✅',
        'warning': '⚠️'
    }
    
    emoji = emoji_map.get(message_type, '📢')
    
    message = f"""
{emoji} *Airflow Alert: {message_type.upper()}*

*DAG*: `{dag_id}`
*Task*: `{task_id}`
*Execution Date*: `{execution_date}`
*Log URL*: {log_url}

*Environment*: Production
*Time*: {context['execution_date']}
"""
    
    return message

def send_failure_alert(context):
    """Send alert on task failure"""
    message = create_slack_alert(context, 'failure')
    
    # Add error details
    if context.get('exception'):
        message += f"\n*Error*: ```{context['exception']}```"
    
    # Send to Slack
    slack_alert = SlackWebhookOperator(
        task_id='slack_failure_alert',
        http_conn_id='slack_webhook',
        message=message,
    )
    
    return slack_alert.execute(context=context)

def send_success_alert(context):
    """Send alert on DAG success"""
    message = create_slack_alert(context, 'success')
    
    slack_alert = SlackWebhookOperator(
        task_id='slack_success_alert',
        http_conn_id='slack_webhook',
        message=message,
    )
    
    return slack_alert.execute(context=context)

# Usage in DAG:
default_args = {
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert,
}
```

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────┐
│           CLOUD COMPOSER QUICK REFERENCE             │
└─────────────────────────────────────────────────────┘

📁 PROJECT STRUCTURE
dags/
  ├── my_dag.py              # Your DAG files
  ├── utils/                 # Shared utilities
  └── sql/                   # SQL queries

🚀 DEPLOYMENT
gsutil cp dags/*.py gs://[BUCKET]/dags/

🧪 TESTING
airflow dags test my_dag 2024-01-15
airflow tasks test my_dag my_task 2024-01-15

📊 MONITORING
Airflow UI → Graph View → Task Logs

🔧 DEBUGGING
python dags/my_dag.py  # Check syntax
pytest tests/          # Run tests

⏰ SCHEDULE FORMATS
@once, @hourly, @daily, @weekly, @monthly
'0 2 * * *'  # 2 AM daily
'*/15 * * * *'  # Every 15 min

🔗 TASK DEPENDENCIES
task1 >> task2 >> task3  # Linear
task1 >> [task2, task3]  # Fan-out
[task1, task2] >> task3  # Fan-in

📦 KEY OPERATORS
BigQueryInsertJobOperator  # Run SQL
GCSToBigQueryOperator      # Load data
PythonOperator             # Run Python
BashOperator               # Run bash

🎯 TEMPLATES
{{ ds }}           # 2024-01-15
{{ ds_nodash }}    # 20240115
{{ dag.dag_id }}   # DAG name
{{ task.task_id }} # Task name
```

---

## Next Steps

1. **Complete the Hello World tutorial** (Section 2)
2. **Deploy one real-world scenario** (Section 3)
3. **Set up monitoring** (Section 5)
4. **Create your own DAG** for your use case
5. **Join Airflow community** for support

**Resources:**
- Airflow Docs: https://airflow.apache.org/docs/
- Cloud Composer Docs: https://cloud.google.com/composer/docs
- Stack Overflow: Tag `apache-airflow`
- GitHub: https://github.com/apache/airflow

**Remember:**
- Start simple, add complexity gradually
- Test locally before deploying
- Monitor everything
- Document your DAGs
- Use version control

Good luck with your Cloud Composer journey! 🎉