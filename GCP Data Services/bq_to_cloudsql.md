# BigQuery to Cloud SQL (MySQL) Migration Guide

## Overview
This guide provides multiple viable options for copying a BigQuery table (500M+ records) from `proj1` to Cloud SQL MySQL in `proj2`.

---

## Option 1: Using Dataflow (Recommended for Large Datasets)

### Why Dataflow?
- Handles 500M+ records efficiently
- Auto-scaling capabilities
- Built-in error handling and retry logic
- Parallel processing

### Prerequisites
1. Enable APIs in both projects
2. Set up service accounts with proper permissions
3. Configure VPC peering/Private IP (if needed)

### Step 1: Environment Setup

```bash
# Set environment variables
export PROJECT1="proj1"
export PROJECT2="proj2"
export BQ_DATASET="your_dataset"
export BQ_TABLE="your_table"
export CLOUDSQL_INSTANCE="your-cloudsql-instance"
export CLOUDSQL_DB="your_database"
export CLOUDSQL_TABLE="your_table"
export REGION="us-central1"
export TEMP_BUCKET="gs://your-temp-bucket"
```

### Step 2: Install Required Packages

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install packages
pip install apache-beam[gcp]==2.53.0
pip install google-cloud-bigquery==3.14.1
pip install google-cloud-storage==2.14.0
pip install pymysql==1.1.0
pip install cryptography==41.0.7
```

Create `requirements.txt`:
```
apache-beam[gcp]==2.53.0
google-cloud-bigquery==3.14.1
google-cloud-storage==2.14.0
pymysql==1.1.0
cryptography==41.0.7
```

### Step 3: Enable Required APIs

```bash
# In proj1
gcloud config set project $PROJECT1
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable compute.googleapis.com

# In proj2
gcloud config set project $PROJECT2
gcloud services enable sqladmin.googleapis.com
gcloud services enable servicenetworking.googleapis.com
```

### Step 4: Set Up Service Account

```bash
# Create service account in proj1
gcloud iam service-accounts create dataflow-bq-to-sql \
    --display-name="Dataflow BQ to CloudSQL"

export SA_EMAIL="dataflow-bq-to-sql@${PROJECT1}.iam.gserviceaccount.com"

# Grant permissions in proj1
gcloud projects add-iam-policy-binding $PROJECT1 \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding $PROJECT1 \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding $PROJECT1 \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectAdmin"

# Grant permissions in proj2
gcloud projects add-iam-policy-binding $PROJECT2 \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/cloudsql.client"
```

### Step 5: Get BigQuery Schema

```bash
# Extract schema
bq show --format=prettyjson ${PROJECT1}:${BQ_DATASET}.${BQ_TABLE} > schema.json
```

### Step 6: Create Cloud SQL Table

First, get the BigQuery schema and convert to MySQL DDL:

```python
# schema_converter.py
from google.cloud import bigquery

def bq_to_mysql_type(bq_type, mode):
    """Convert BigQuery type to MySQL type"""
    type_map = {
        'STRING': 'VARCHAR(255)',
        'INTEGER': 'BIGINT',
        'FLOAT': 'DOUBLE',
        'BOOLEAN': 'BOOLEAN',
        'TIMESTAMP': 'DATETIME',
        'DATE': 'DATE',
        'DATETIME': 'DATETIME',
        'TIME': 'TIME',
        'BYTES': 'BLOB',
        'NUMERIC': 'DECIMAL(38,9)',
        'BIGNUMERIC': 'DECIMAL(76,38)',
    }
    
    mysql_type = type_map.get(bq_type, 'TEXT')
    
    if mode == 'REPEATED':
        return 'JSON'  # Store arrays as JSON
    
    return mysql_type

def generate_mysql_ddl(project, dataset, table):
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"
    table_obj = client.get_table(table_ref)
    
    columns = []
    for field in table_obj.schema:
        col_name = field.name
        col_type = bq_to_mysql_type(field.field_type, field.mode)
        nullable = "NULL" if field.mode == "NULLABLE" else "NOT NULL"
        columns.append(f"  `{col_name}` {col_type} {nullable}")
    
    ddl = f"CREATE TABLE `{table}` (\n"
    ddl += ",\n".join(columns)
    ddl += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    
    return ddl

if __name__ == "__main__":
    import sys
    project = sys.argv[1]
    dataset = sys.argv[2]
    table = sys.argv[3]
    
    print(generate_mysql_ddl(project, dataset, table))
```

Run the converter:
```bash
python schema_converter.py $PROJECT1 $BQ_DATASET $BQ_TABLE > create_table.sql

# Connect to Cloud SQL and create table
gcloud sql connect $CLOUDSQL_INSTANCE --user=root --project=$PROJECT2

# Then run the SQL from create_table.sql
```

### Step 7: Create Dataflow Pipeline

```python
# bq_to_cloudsql_dataflow.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
import pymysql
import logging
import json

class WriteToCloudSQL(beam.DoFn):
    def __init__(self, instance_connection_name, database, table, user, password):
        self.instance_connection_name = instance_connection_name
        self.database = database
        self.table = table
        self.user = user
        self.password = password
        self.connection = None
        
    def setup(self):
        """Initialize connection once per worker"""
        import pymysql
        
        # For public IP
        self.connection = pymysql.connect(
            host='<CLOUD_SQL_IP>',  # Replace with actual IP
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
    def process(self, element):
        """Process each record"""
        try:
            cursor = self.connection.cursor()
            
            # Build INSERT statement dynamically
            columns = list(element.keys())
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join([f'`{col}`' for col in columns])
            
            sql = f"INSERT INTO `{self.table}` ({column_names}) VALUES ({placeholders})"
            
            # Handle nested/repeated fields (convert to JSON)
            values = []
            for col in columns:
                val = element[col]
                if isinstance(val, (list, dict)):
                    values.append(json.dumps(val))
                else:
                    values.append(val)
            
            cursor.execute(sql, values)
            self.connection.commit()
            cursor.close()
            
            yield element
            
        except Exception as e:
            logging.error(f"Error inserting record: {e}")
            logging.error(f"Record: {element}")
            
    def teardown(self):
        """Close connection"""
        if self.connection:
            self.connection.close()

class BatchWriteToCloudSQL(beam.DoFn):
    """More efficient batch writer"""
    def __init__(self, instance_connection_name, database, table, user, password, batch_size=1000):
        self.instance_connection_name = instance_connection_name
        self.database = database
        self.table = table
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.connection = None
        self.batch = []
        
    def setup(self):
        import pymysql
        self.connection = pymysql.connect(
            host='<CLOUD_SQL_IP>',
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
    def process(self, element):
        self.batch.append(element)
        
        if len(self.batch) >= self.batch_size:
            self._flush_batch()
            
        yield element
        
    def _flush_batch(self):
        if not self.batch:
            return
            
        try:
            cursor = self.connection.cursor()
            
            # Build batch INSERT
            first_row = self.batch[0]
            columns = list(first_row.keys())
            column_names = ', '.join([f'`{col}`' for col in columns])
            placeholders = ', '.join(['%s'] * len(columns))
            
            sql = f"INSERT INTO `{self.table}` ({column_names}) VALUES ({placeholders})"
            
            # Prepare all values
            batch_values = []
            for element in self.batch:
                values = []
                for col in columns:
                    val = element[col]
                    if isinstance(val, (list, dict)):
                        values.append(json.dumps(val))
                    else:
                        values.append(val)
                batch_values.append(values)
            
            cursor.executemany(sql, batch_values)
            self.connection.commit()
            cursor.close()
            
            logging.info(f"Inserted batch of {len(self.batch)} records")
            self.batch = []
            
        except Exception as e:
            logging.error(f"Error inserting batch: {e}")
            
    def finish_bundle(self):
        self._flush_batch()
        
    def teardown(self):
        self._flush_batch()
        if self.connection:
            self.connection.close()

def run():
    # Pipeline options
    options = PipelineOptions()
    
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'proj1'
    google_cloud_options.job_name = 'bq-to-cloudsql-migration'
    google_cloud_options.staging_location = 'gs://your-temp-bucket/staging'
    google_cloud_options.temp_location = 'gs://your-temp-bucket/temp'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.service_account_email = 'dataflow-bq-to-sql@proj1.iam.gserviceaccount.com'
    
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = 'n1-standard-4'
    worker_options.max_num_workers = 50
    worker_options.disk_size_gb = 100
    
    # Configuration
    BQ_TABLE = 'proj1:your_dataset.your_table'
    CLOUDSQL_INSTANCE = 'proj2:us-central1:your-instance'
    CLOUDSQL_DB = 'your_database'
    CLOUDSQL_TABLE = 'your_table'
    CLOUDSQL_USER = 'root'
    CLOUDSQL_PASSWORD = 'your-password'  # Use Secret Manager in production
    
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Read from BigQuery' >> ReadFromBigQuery(
                query=f'SELECT * FROM `{BQ_TABLE}`',
                use_standard_sql=True
            )
            | 'Write to Cloud SQL' >> beam.ParDo(
                BatchWriteToCloudSQL(
                    CLOUDSQL_INSTANCE,
                    CLOUDSQL_DB,
                    CLOUDSQL_TABLE,
                    CLOUDSQL_USER,
                    CLOUDSQL_PASSWORD,
                    batch_size=1000
                )
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
```

### Step 8: Run the Pipeline

```bash
# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# Run locally for testing (small dataset)
python bq_to_cloudsql_dataflow.py \
    --runner DirectRunner

# Run on Dataflow
python bq_to_cloudsql_dataflow.py \
    --runner DataflowRunner \
    --project proj1 \
    --region us-central1 \
    --temp_location gs://your-temp-bucket/temp \
    --staging_location gs://your-temp-bucket/staging \
    --service_account_email dataflow-bq-to-sql@proj1.iam.gserviceaccount.com \
    --machine_type n1-standard-4 \
    --max_num_workers 50
```

---

## Option 2: Using Cloud Composer (Airflow)

### Why Cloud Composer?
- Orchestration and scheduling
- Better monitoring and logging
- Retry and error handling
- Can chunk the work into manageable pieces

### Step 1: Set Up Cloud Composer

```bash
# Create Composer environment (in proj1)
gcloud composer environments create bq-to-cloudsql-env \
    --location us-central1 \
    --zone us-central1-a \
    --machine-type n1-standard-4 \
    --disk-size 30 \
    --python-version 3 \
    --project $PROJECT1
```

### Step 2: Install PyPI Packages

```bash
# Get the environment name
export COMPOSER_ENV="bq-to-cloudsql-env"
export COMPOSER_LOCATION="us-central1"

# Install packages
gcloud composer environments update $COMPOSER_ENV \
    --location $COMPOSER_LOCATION \
    --update-pypi-packages-from-file requirements.txt \
    --project $PROJECT1
```

`requirements.txt` for Composer:
```
google-cloud-bigquery==3.14.1
pymysql==1.1.0
pandas==2.0.3
pandas-gbq==0.19.2
sqlalchemy==2.0.23
google-cloud-secret-manager==2.16.4
```

### Step 3: Store Credentials in Secret Manager

```bash
# Store Cloud SQL password
echo -n "your-cloudsql-password" | gcloud secrets create cloudsql-password \
    --data-file=- \
    --replication-policy="automatic" \
    --project=$PROJECT2

# Grant access to Composer service account
export COMPOSER_SA=$(gcloud composer environments describe $COMPOSER_ENV \
    --location $COMPOSER_LOCATION \
    --project $PROJECT1 \
    --format="get(config.nodeConfig.serviceAccount)")

gcloud secrets add-iam-policy-binding cloudsql-password \
    --member="serviceAccount:${COMPOSER_SA}" \
    --role="roles/secretmanager.secretAccessor" \
    --project=$PROJECT2
```

### Step 4: Create Airflow DAG

```python
# dags/bq_to_cloudsql_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from google.cloud import secretmanager
import pymysql
import pandas as pd
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
PROJECT1 = 'proj1'
PROJECT2 = 'proj2'
BQ_DATASET = 'your_dataset'
BQ_TABLE = 'your_table'
CLOUDSQL_HOST = '<CLOUD_SQL_IP>'
CLOUDSQL_DB = 'your_database'
CLOUDSQL_TABLE = 'your_table'
CLOUDSQL_USER = 'root'
CHUNK_SIZE = 100000  # Process 100k rows at a time

def get_secret(project_id, secret_id, version_id="latest"):
    """Retrieve secret from Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_total_rows(**context):
    """Get total number of rows in BigQuery table"""
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    
    query = f"""
    SELECT COUNT(*) as total_rows
    FROM `{PROJECT1}.{BQ_DATASET}.{BQ_TABLE}`
    """
    
    df = bq_hook.get_pandas_df(sql=query, dialect='standard')
    total_rows = int(df['total_rows'].iloc[0])
    
    logging.info(f"Total rows in BigQuery table: {total_rows}")
    context['ti'].xcom_push(key='total_rows', value=total_rows)
    
    return total_rows

def create_cloudsql_table(**context):
    """Create table in Cloud SQL if not exists"""
    password = get_secret(PROJECT2, 'cloudsql-password')
    
    connection = pymysql.connect(
        host=CLOUDSQL_HOST,
        user=CLOUDSQL_USER,
        password=password,
        database=CLOUDSQL_DB,
        charset='utf8mb4'
    )
    
    try:
        cursor = connection.cursor()
        
        # Get schema from BigQuery
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        table = bq_hook.get_client().get_table(f"{PROJECT1}.{BQ_DATASET}.{BQ_TABLE}")
        
        # Generate CREATE TABLE statement
        columns = []
        for field in table.schema:
            type_map = {
                'STRING': 'VARCHAR(255)',
                'INTEGER': 'BIGINT',
                'FLOAT': 'DOUBLE',
                'BOOLEAN': 'BOOLEAN',
                'TIMESTAMP': 'DATETIME',
                'DATE': 'DATE',
                'DATETIME': 'DATETIME',
            }
            
            col_type = type_map.get(field.field_type, 'TEXT')
            nullable = "NULL" if field.mode == "NULLABLE" else "NOT NULL"
            columns.append(f"`{field.name}` {col_type} {nullable}")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{CLOUDSQL_TABLE}` (
            {', '.join(columns)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        cursor.execute(create_sql)
        connection.commit()
        logging.info("Table created successfully")
        
    finally:
        connection.close()

def migrate_chunk(offset, limit, **context):
    """Migrate a chunk of data from BigQuery to Cloud SQL"""
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    password = get_secret(PROJECT2, 'cloudsql-password')
    
    # Read from BigQuery
    query = f"""
    SELECT *
    FROM `{PROJECT1}.{BQ_DATASET}.{BQ_TABLE}`
    LIMIT {limit}
    OFFSET {offset}
    """
    
    logging.info(f"Reading rows {offset} to {offset + limit}")
    df = bq_hook.get_pandas_df(sql=query, dialect='standard')
    
    if df.empty:
        logging.info("No data to migrate in this chunk")
        return
    
    # Connect to Cloud SQL
    connection = pymysql.connect(
        host=CLOUDSQL_HOST,
        user=CLOUDSQL_USER,
        password=password,
        database=CLOUDSQL_DB,
        charset='utf8mb4'
    )
    
    try:
        cursor = connection.cursor()
        
        # Prepare batch insert
        columns = df.columns.tolist()
        column_names = ', '.join([f'`{col}`' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f"INSERT INTO `{CLOUDSQL_TABLE}` ({column_names}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in df.values]
        
        # Batch insert
        cursor.executemany(insert_sql, data)
        connection.commit()
        
        logging.info(f"Inserted {len(df)} rows (offset {offset})")
        
    finally:
        connection.close()

def generate_migration_tasks(total_rows):
    """Generate task IDs for dynamic task mapping"""
    num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
    return [{'offset': i * CHUNK_SIZE, 'limit': CHUNK_SIZE} for i in range(num_chunks)]

with DAG(
    'bq_to_cloudsql_migration',
    default_args=default_args,
    description='Migrate data from BigQuery to Cloud SQL',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:
    
    count_rows = PythonOperator(
        task_id='count_rows',
        python_callable=get_total_rows,
        provide_context=True,
    )
    
    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_cloudsql_table,
        provide_context=True,
    )
    
    # For Airflow 2.3+, use dynamic task mapping
    # For older versions, create tasks in a loop
    
    # Example: Create 10 parallel migration tasks
    migration_tasks = []
    for i in range(10):  # Adjust based on your needs
        offset = i * CHUNK_SIZE
        task = PythonOperator(
            task_id=f'migrate_chunk_{i}',
            python_callable=migrate_chunk,
            op_kwargs={'offset': offset, 'limit': CHUNK_SIZE},
            provide_context=True,
        )
        migration_tasks.append(task)
    
    count_rows >> create_table >> migration_tasks
```

### Step 5: Upload DAG to Composer

```bash
# Get the DAGs folder
export DAGS_FOLDER=$(gcloud composer environments describe $COMPOSER_ENV \
    --location $COMPOSER_LOCATION \
    --project $PROJECT1 \
    --format="get(config.dagGcsPrefix)")

# Upload DAG
gsutil cp dags/bq_to_cloudsql_dag.py $DAGS_FOLDER/
```

### Step 6: Trigger the DAG

```bash
# Via gcloud
gcloud composer environments run $COMPOSER_ENV \
    --location $COMPOSER_LOCATION \
    dags trigger -- bq_to_cloudsql_migration \
    --project $PROJECT1

# Or use the Airflow UI
```

---

## Option 3: Export to GCS + LOAD DATA INFILE

### Why This Method?
- Fastest for bulk loading
- Simple and straightforward
- Good for one-time migrations

### Step 1: Export BigQuery to Cloud Storage

```bash
# Export as CSV (in chunks if needed)
bq extract \
    --project_id=$PROJECT1 \
    --destination_format=CSV \
    --field_delimiter=',' \
    --print_header=true \
    ${BQ_DATASET}.${BQ_TABLE} \
    gs://your-temp-bucket/export/table-*.csv
```

### Step 2: Import to Cloud SQL

```bash
# Import from GCS to Cloud SQL
gcloud sql import csv $CLOUDSQL_INSTANCE \
    gs://your-temp-bucket/export/table-*.csv \
    --database=$CLOUDSQL_DB \
    --table=$CLOUDSQL_TABLE \
    --project=$PROJECT2
```

---

## Option 4: Python Script with Batching

### Simple Python Migration Script

```python
# migrate.py
from google.cloud import bigquery
import pymysql
import logging
from google.cloud import secretmanager

# Configuration
PROJECT1 = 'proj1'
BQ_DATASET = 'your_dataset'
BQ_TABLE = 'your_table'
CLOUDSQL_HOST = '<CLOUD_SQL_IP>'
CLOUDSQL_DB = 'your_database'
CLOUDSQL_TABLE = 'your_table'
CLOUDSQL_USER = 'root'
BATCH_SIZE = 10000

def get_secret(project_id, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def migrate():
    # Initialize clients
    bq_client = bigquery.Client(project=PROJECT1)
    password = get_secret('proj2', 'cloudsql-password')
    
    sql_conn = pymysql.connect(
        host=CLOUDSQL_HOST,
        user=CLOUDSQL_USER,
        password=password,
        database=CLOUDSQL_DB,
        charset='utf8mb4'
    )
    
    try:
        # Query BigQuery in batches
        query = f"""
        SELECT *
        FROM `{PROJECT1}.{BQ_DATASET}.{BQ_TABLE}`
        """
        
        query_job = bq_client.query(query)
        
        cursor = sql_conn.cursor()
        batch = []
        total_rows = 0
        
        for row in query_job:
            # Convert row to dict
            row_dict = dict(row.items())
            batch.append(row_dict)
            
            if len(batch) >= BATCH_SIZE:
                insert_batch(cursor, CLOUDSQL_TABLE, batch)
                sql_conn.commit()
                total_rows += len(batch)
                logging.info(f"Inserted {total_rows} rows")
                batch = []
        
        # Insert remaining rows
        if batch:
            insert_batch(cursor, CLOUDSQL_TABLE, batch)
            sql_conn.commit()
            total_rows += len(batch)
        
        logging.info(f"Migration complete. Total rows: {total_rows}")
        
    finally:
        sql_conn.close()

def insert_batch(cursor, table, batch):
    if not batch:
        return
    
    columns = list(batch[0].keys())
    column_names = ', '.join([f'`{col}`' for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    
    sql = f"INSERT INTO `{table}` ({column_names}) VALUES ({placeholders})"
    
    values = [tuple(row[col] for col in columns) for row in batch]
    cursor.executemany(sql, values)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    migrate()
```

Run the script:
```bash
python migrate.py
```

---

## Comparison Matrix

| Method | Best For | Speed | Complexity | Cost | Fault Tolerance |
|--------|----------|-------|------------|------|-----------------|
| Dataflow | Large datasets (500M+ rows) | Fast | Medium | Medium-High | Excellent |
| Cloud Composer | Scheduled/orchestrated migrations | Medium | High | High | Excellent |
| GCS Export + Import | One-time bulk load | Fastest | Low | Low | Poor |
| Python Script | Small-medium datasets | Slow | Low | Low | Poor |

---

## Recommendations

For **500M+ records**, I recommend:

1. **Primary**: Dataflow (Option 1) - Best balance of speed, reliability, and cost
2. **Alternative**: Cloud Composer (Option 2) - If you need scheduling and better monitoring
3. **Quick & Dirty**: GCS Export (Option 3) - For one-time migration, fastest but less fault-tolerant

---

## Important Considerations

### Performance Optimization
- Add indexes on Cloud SQL table AFTER data load
- Use batching (1000-10000 rows per batch)
- Consider partitioning for very large tables
- Monitor Cloud SQL connection limits

### Security
- Use Secret Manager for credentials
- Enable VPC peering between projects
- Use private IP for Cloud SQL
- Restrict service account permissions

### Monitoring
- Set up Cloud Monitoring alerts
- Monitor Cloud SQL CPU/Memory/Connections
- Track Dataflow job progress
- Log failed records for retry

### Cost Management
- Use preemptible workers in Dataflow
- Clean up GCS temp files
- Right-size Cloud SQL instance
- Consider BigQuery export costs
