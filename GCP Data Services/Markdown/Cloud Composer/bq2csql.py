from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# BQ to MySQL type mapping
BQ_TO_MYSQL_TYPE_MAP = {
    'STRING': 'TEXT',
    'BYTES': 'BLOB',
    'INTEGER': 'BIGINT',
    'INT64': 'BIGINT',
    'FLOAT': 'DOUBLE',
    'FLOAT64': 'DOUBLE',
    'NUMERIC': 'DECIMAL(38,9)',
    'BIGNUMERIC': 'DECIMAL(76,38)',
    'BOOLEAN': 'BOOLEAN',
    'BOOL': 'BOOLEAN',
    'TIMESTAMP': 'TIMESTAMP',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'DATETIME': 'DATETIME',
}

def create_table_from_bq_schema(**context):
    """Create CloudSQL table based on BQ table schema"""
    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default')
    mysql_hook = MySqlHook(mysql_conn_id='cloudsql_proj2_connection')
    
    # Get BQ table schema
    bq_table_ref = 'proj1.dataset_name.table_name'  # Update this
    project, dataset, table = bq_table_ref.split('.')
    
    table_obj = bq_hook.get_client(project_id=project).get_table(f"{project}.{dataset}.{table}")
    
    # Build CREATE TABLE statement
    columns = []
    for field in table_obj.schema:
        field_type = BQ_TO_MYSQL_TYPE_MAP.get(field.field_type, 'TEXT')
        null_constraint = 'NOT NULL' if field.mode == 'REQUIRED' else 'NULL'
        columns.append(f"`{field.name}` {field_type} {null_constraint}")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS target_table_name (
        {', '.join(columns)}
    )
    """
    
    # Execute CREATE TABLE
    mysql_hook.run(create_table_sql)
    print(f"Table created successfully: {create_table_sql}")

def copy_data_from_bq(**context):
    """Copy data from BQ to CloudSQL"""
    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default')
    mysql_hook = MySqlHook(mysql_conn_id='cloudsql_proj2_connection')
    
    # Query BQ data
    bq_table_ref = 'proj1.dataset_name.table_name'  # Update this
    sql = f"SELECT * FROM `{bq_table_ref}`"
    
    df = bq_hook.get_pandas_df(sql=sql, dialect='standard')
    
    if len(df) > 0:
        # Clear existing data
        mysql_hook.run("TRUNCATE TABLE target_table_name")
        
        # Insert data in batches
        conn = mysql_hook.get_conn()
        df.to_sql('target_table_name', conn, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f"Copied {len(df)} rows to CloudSQL")
    else:
        print("No data to copy")

# Define the DAG
with DAG(
    dag_id='bq_to_cloudsql_copy',
    default_args=default_args,
    description='Copy BQ table from proj1 to CloudSQL in proj2',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['bigquery', 'cloudsql', 'transfer'],
) as dag:

    # Task 1: Create table in CloudSQL with BQ schema
    create_table = PythonOperator(
        task_id='create_cloudsql_table',
        python_callable=create_table_from_bq_schema,
    )
    
    # Task 2: Copy data from BQ to CloudSQL
    copy_data = PythonOperator(
        task_id='copy_data',
        python_callable=copy_data_from_bq,
    )
    
    create_table >> copy_data
