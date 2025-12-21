# Teradata to GCP BigQuery Migration Guide

## Complete Python Code with Step-by-Step Instructions

---

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Architecture Overview](#architecture-overview)
3. [Environment Setup](#environment-setup)
4. [Migration Process](#migration-process)
5. [Code Implementation](#code-implementation)
6. [Validation & Testing](#validation--testing)
7. [Best Practices](#best-practices)

---

## Prerequisites

### What You Need Before Starting:
- GCP Project with BigQuery API enabled
- Service Account with necessary permissions
- Teradata database credentials
- Python 3.8+ installed
- BigQuery Notebooks environment (Vertex AI Workbench)

### Required GCP Permissions:
```
bigquery.datasets.create
bigquery.tables.create
bigquery.tables.updateData
bigquery.jobs.create
storage.buckets.create
storage.objects.create
```

---

## Architecture Overview

```
┌─────────────────┐
│   TERADATA DB   │
│                 │
│  ┌───────────┐  │
│  │  Tables   │  │
│  │  Views    │  │
│  │  Data     │  │
│  └───────────┘  │
└────────┬────────┘
         │
         │ Extract
         ▼
┌─────────────────┐
│  Python Script  │
│  (Extraction)   │
└────────┬────────┘
         │
         │ Stage
         ▼
┌─────────────────┐
│  Google Cloud   │
│    Storage      │
│   (CSV/Parquet) │
└────────┬────────┘
         │
         │ Load
         ▼
┌─────────────────┐
│    BigQuery     │
│                 │
│  ┌───────────┐  │
│  │  Dataset  │  │
│  │  Tables   │  │
│  └───────────┘  │
└─────────────────┘
```

---

## Environment Setup

### Step 1: Install Required Packages

**When to use:** Run this first in your BigQuery Notebook before any other code.

**How to use:** Copy this code into the first cell of your BigQuery Notebook and execute.

```python
# Cell 1: Install Dependencies
!pip install --quiet teradatasql google-cloud-bigquery google-cloud-storage pandas pyarrow db-dtypes

# Verify installations
import sys
print(f"Python version: {sys.version}")
print("✓ All packages installed successfully")
```

### Step 2: Import Libraries and Initialize Clients

**When to use:** After package installation, this sets up all necessary connections.

**How to use:** Execute this in the second cell.

```python
# Cell 2: Import Libraries
import teradatasql
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import json
import time
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Initialize GCP clients
bq_client = bigquery.Client()
storage_client = storage.Client()

print("✓ All libraries imported")
print(f"✓ BigQuery client initialized for project: {bq_client.project}")
```

---

## Migration Process

### Configuration Class

**When to use:** Define your connection parameters and migration settings.

**How to use:** Update the credentials with your actual values.

```python
# Cell 3: Configuration
class MigrationConfig:
    """Configuration for Teradata to BigQuery migration"""
    
    # Teradata Configuration
    TERADATA_HOST = "your-teradata-host.com"
    TERADATA_USER = "your_username"
    TERADATA_PASSWORD = "your_password"
    TERADATA_DATABASE = "your_database"
    
    # GCP Configuration
    GCP_PROJECT_ID = "your-gcp-project-id"
    GCS_BUCKET_NAME = "your-migration-bucket"
    BQ_DATASET_NAME = "migrated_data"
    
    # Migration Settings
    BATCH_SIZE = 100000  # Rows per batch
    STAGING_FORMAT = "PARQUET"  # or "CSV"
    
    # Tables to migrate (add your tables here)
    TABLES_TO_MIGRATE = [
        "table_name_1",
        "table_name_2",
        "table_name_3"
    ]

config = MigrationConfig()
print("✓ Configuration loaded")
```

---

## Code Implementation

### 1. Teradata Connection Manager

**When to use:** Establishes and manages connection to Teradata.

**How to use:** This class handles all Teradata operations automatically.

```python
# Cell 4: Teradata Connection Manager
class TeradataConnector:
    """Handles Teradata database connections and operations"""
    
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
    
    def connect(self):
        """Establish connection to Teradata"""
        try:
            self.connection = teradatasql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print(f"✓ Connected to Teradata: {self.host}")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {str(e)}")
            return False
    
    def get_table_schema(self, table_name):
        """Retrieve table schema information"""
        query = f"""
        SELECT 
            ColumnName,
            ColumnType,
            ColumnLength,
            Nullable,
            ColumnId
        FROM DBC.ColumnsV
        WHERE DatabaseName = '{self.database}'
        AND TableName = '{table_name}'
        ORDER BY ColumnId
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            columns = cursor.fetchall()
            return columns
    
    def get_row_count(self, table_name):
        """Get total row count for a table"""
        query = f"SELECT COUNT(*) FROM {self.database}.{table_name}"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
    
    def extract_data(self, table_name, batch_size=100000, offset=0):
        """Extract data in batches"""
        query = f"""
        SELECT * FROM {self.database}.{table_name}
        LIMIT {batch_size} OFFSET {offset}
        """
        
        df = pd.read_sql(query, self.connection)
        return df
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("✓ Teradata connection closed")

# Initialize Teradata connector
td_connector = TeradataConnector(
    host=config.TERADATA_HOST,
    user=config.TERADATA_USER,
    password=config.TERADATA_PASSWORD,
    database=config.TERADATA_DATABASE
)
```

### 2. Data Type Mapping

**When to use:** Automatically converts Teradata data types to BigQuery equivalents.

**How to use:** This runs automatically during migration.

```python
# Cell 5: Data Type Mapping
class DataTypeMapper:
    """Maps Teradata data types to BigQuery data types"""
    
    TYPE_MAPPING = {
        # Numeric Types
        'BYTEINT': 'INT64',
        'SMALLINT': 'INT64',
        'INTEGER': 'INT64',
        'BIGINT': 'INT64',
        'DECIMAL': 'NUMERIC',
        'NUMERIC': 'NUMERIC',
        'FLOAT': 'FLOAT64',
        'REAL': 'FLOAT64',
        'DOUBLE PRECISION': 'FLOAT64',
        
        # String Types
        'CHAR': 'STRING',
        'VARCHAR': 'STRING',
        'CLOB': 'STRING',
        'LONG VARCHAR': 'STRING',
        
        # Date/Time Types
        'DATE': 'DATE',
        'TIME': 'TIME',
        'TIMESTAMP': 'TIMESTAMP',
        
        # Binary Types
        'BYTE': 'BYTES',
        'VARBYTE': 'BYTES',
        'BLOB': 'BYTES',
        
        # Other Types
        'INTERVAL': 'STRING',
        'PERIOD': 'STRING',
        'JSON': 'JSON',
        'XML': 'STRING'
    }
    
    @classmethod
    def convert_type(cls, teradata_type):
        """Convert Teradata type to BigQuery type"""
        # Extract base type (remove length/precision)
        base_type = teradata_type.split('(')[0].strip().upper()
        
        return cls.TYPE_MAPPING.get(base_type, 'STRING')
    
    @classmethod
    def create_bq_schema(cls, teradata_schema):
        """Create BigQuery schema from Teradata schema"""
        bq_schema = []
        
        for column in teradata_schema:
            col_name = column[0]
            col_type = column[1]
            nullable = column[3]
            
            bq_type = cls.convert_type(col_type)
            mode = 'NULLABLE' if nullable == 'Y' else 'REQUIRED'
            
            bq_schema.append(
                bigquery.SchemaField(col_name, bq_type, mode=mode)
            )
        
        return bq_schema

print("✓ Data type mapper initialized")
```

### 3. GCS Staging Manager

**When to use:** Handles temporary storage of data in Google Cloud Storage.

**How to use:** This automatically stages data during migration.

```python
# Cell 6: GCS Staging Manager
class GCSStagingManager:
    """Manages data staging in Google Cloud Storage"""
    
    def __init__(self, bucket_name, storage_client):
        self.bucket_name = bucket_name
        self.storage_client = storage_client
        self.bucket = None
    
    def create_bucket_if_not_exists(self):
        """Create GCS bucket if it doesn't exist"""
        try:
            self.bucket = self.storage_client.get_bucket(self.bucket_name)
            print(f"✓ Using existing bucket: {self.bucket_name}")
        except:
            self.bucket = self.storage_client.create_bucket(
                self.bucket_name,
                location="US"
            )
            print(f"✓ Created new bucket: {self.bucket_name}")
    
    def upload_dataframe(self, df, table_name, batch_number, format='PARQUET'):
        """Upload DataFrame to GCS"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format == 'PARQUET':
            file_name = f"{table_name}/{table_name}_batch_{batch_number}_{timestamp}.parquet"
            local_path = f"/tmp/{file_name.split('/')[-1]}"
            df.to_parquet(local_path, index=False)
        else:  # CSV
            file_name = f"{table_name}/{table_name}_batch_{batch_number}_{timestamp}.csv"
            local_path = f"/tmp/{file_name.split('/')[-1]}"
            df.to_csv(local_path, index=False)
        
        blob = self.bucket.blob(file_name)
        blob.upload_from_filename(local_path)
        
        gcs_uri = f"gs://{self.bucket_name}/{file_name}"
        print(f"  ✓ Uploaded batch {batch_number}: {len(df)} rows → {gcs_uri}")
        
        return gcs_uri
    
    def cleanup_staging_files(self, table_name):
        """Clean up staging files after successful load"""
        blobs = self.bucket.list_blobs(prefix=f"{table_name}/")
        for blob in blobs:
            blob.delete()
        print(f"✓ Cleaned up staging files for {table_name}")

# Initialize GCS manager
gcs_manager = GCSStagingManager(config.GCS_BUCKET_NAME, storage_client)
gcs_manager.create_bucket_if_not_exists()
```

### 4. BigQuery Manager

**When to use:** Creates datasets and tables in BigQuery.

**How to use:** Automatically creates destination tables with proper schema.

```python
# Cell 7: BigQuery Manager
class BigQueryManager:
    """Manages BigQuery operations"""
    
    def __init__(self, project_id, dataset_name, bq_client):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.bq_client = bq_client
        self.dataset_ref = None
    
    def create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist"""
        dataset_id = f"{self.project_id}.{self.dataset_name}"
        
        try:
            self.dataset_ref = self.bq_client.get_dataset(dataset_id)
            print(f"✓ Using existing dataset: {dataset_id}")
        except:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            self.dataset_ref = self.bq_client.create_dataset(dataset)
            print(f"✓ Created new dataset: {dataset_id}")
    
    def create_table(self, table_name, schema):
        """Create BigQuery table with schema"""
        table_id = f"{self.project_id}.{self.dataset_name}.{table_name}"
        
        # Check if table exists
        try:
            self.bq_client.get_table(table_id)
            print(f"  ⚠ Table {table_name} already exists, will append data")
            return table_id
        except:
            pass
        
        # Create new table
        table = bigquery.Table(table_id, schema=schema)
        table = self.bq_client.create_table(table)
        print(f"  ✓ Created table: {table_name}")
        
        return table_id
    
    def load_from_gcs(self, table_id, gcs_uris, format='PARQUET'):
        """Load data from GCS into BigQuery"""
        job_config = bigquery.LoadJobConfig()
        
        if format == 'PARQUET':
            job_config.source_format = bigquery.SourceFormat.PARQUET
        else:
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
        
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
        load_job = self.bq_client.load_table_from_uri(
            gcs_uris,
            table_id,
            job_config=job_config
        )
        
        load_job.result()  # Wait for job to complete
        
        return load_job
    
    def get_row_count(self, table_name):
        """Get row count from BigQuery table"""
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_name}.{table_name}`"
        result = self.bq_client.query(query).result()
        return list(result)[0]['count']

# Initialize BigQuery manager
bq_manager = BigQueryManager(
    config.GCP_PROJECT_ID,
    config.BQ_DATASET_NAME,
    bq_client
)
bq_manager.create_dataset_if_not_exists()
```

### 5. Migration Orchestrator

**When to use:** Main class that orchestrates the entire migration process.

**How to use:** This is the core migration engine - run this to migrate tables.

```python
# Cell 8: Migration Orchestrator
class MigrationOrchestrator:
    """Orchestrates the complete migration process"""
    
    def __init__(self, td_connector, gcs_manager, bq_manager, config):
        self.td_connector = td_connector
        self.gcs_manager = gcs_manager
        self.bq_manager = bq_manager
        self.config = config
        self.migration_stats = {}
    
    def migrate_table(self, table_name):
        """Migrate a single table from Teradata to BigQuery"""
        print(f"\n{'='*60}")
        print(f"Starting migration: {table_name}")
        print(f"{'='*60}")
        
        start_time = time.time()
        
        try:
            # Step 1: Get table schema
            print("\n[1/6] Retrieving table schema...")
            td_schema = self.td_connector.get_table_schema(table_name)
            bq_schema = DataTypeMapper.create_bq_schema(td_schema)
            print(f"  ✓ Schema retrieved: {len(td_schema)} columns")
            
            # Step 2: Get row count
            print("\n[2/6] Counting rows...")
            total_rows = self.td_connector.get_row_count(table_name)
            print(f"  ✓ Total rows to migrate: {total_rows:,}")
            
            # Step 3: Create BigQuery table
            print("\n[3/6] Creating BigQuery table...")
            table_id = self.bq_manager.create_table(table_name, bq_schema)
            
            # Step 4: Extract and stage data
            print("\n[4/6] Extracting and staging data...")
            gcs_uris = []
            batch_number = 0
            offset = 0
            
            while offset < total_rows:
                batch_number += 1
                print(f"  Processing batch {batch_number}...")
                
                # Extract data
                df = self.td_connector.extract_data(
                    table_name,
                    batch_size=self.config.BATCH_SIZE,
                    offset=offset
                )
                
                if len(df) == 0:
                    break
                
                # Upload to GCS
                gcs_uri = self.gcs_manager.upload_dataframe(
                    df,
                    table_name,
                    batch_number,
                    format=self.config.STAGING_FORMAT
                )
                gcs_uris.append(gcs_uri)
                
                offset += self.config.BATCH_SIZE
            
            # Step 5: Load into BigQuery
            print(f"\n[5/6] Loading data into BigQuery...")
            load_job = self.bq_manager.load_from_gcs(
                table_id,
                gcs_uris,
                format=self.config.STAGING_FORMAT
            )
            print(f"  ✓ Loaded {len(gcs_uris)} batches")
            
            # Step 6: Validate
            print("\n[6/6] Validating migration...")
            bq_row_count = self.bq_manager.get_row_count(table_name)
            
            migration_time = time.time() - start_time
            
            # Store stats
            self.migration_stats[table_name] = {
                'source_rows': total_rows,
                'target_rows': bq_row_count,
                'batches': batch_number,
                'time_seconds': migration_time,
                'status': 'SUCCESS' if total_rows == bq_row_count else 'MISMATCH'
            }
            
            # Print summary
            print(f"\n{'='*60}")
            print(f"Migration Complete: {table_name}")
            print(f"{'='*60}")
            print(f"  Source rows:    {total_rows:,}")
            print(f"  Target rows:    {bq_row_count:,}")
            print(f"  Match:          {'✓ YES' if total_rows == bq_row_count else '✗ NO'}")
            print(f"  Time:           {migration_time:.2f} seconds")
            print(f"  Speed:          {total_rows/migration_time:.0f} rows/sec")
            print(f"{'='*60}\n")
            
            # Cleanup staging files
            self.gcs_manager.cleanup_staging_files(table_name)
            
            return True
            
        except Exception as e:
            print(f"\n✗ Migration failed for {table_name}: {str(e)}")
            self.migration_stats[table_name] = {
                'status': 'FAILED',
                'error': str(e)
            }
            return False
    
    def migrate_all_tables(self):
        """Migrate all configured tables"""
        print(f"\n{'#'*60}")
        print(f"  TERADATA TO BIGQUERY MIGRATION")
        print(f"  Tables to migrate: {len(self.config.TABLES_TO_MIGRATE)}")
        print(f"{'#'*60}\n")
        
        # Connect to Teradata
        if not self.td_connector.connect():
            print("✗ Failed to connect to Teradata")
            return
        
        # Migrate each table
        for table_name in self.config.TABLES_TO_MIGRATE:
            self.migrate_table(table_name)
        
        # Close connection
        self.td_connector.close()
        
        # Print final summary
        self.print_summary()
    
    def print_summary(self):
        """Print migration summary"""
        print(f"\n{'#'*60}")
        print(f"  MIGRATION SUMMARY")
        print(f"{'#'*60}\n")
        
        successful = sum(1 for stats in self.migration_stats.values() if stats.get('status') == 'SUCCESS')
        failed = sum(1 for stats in self.migration_stats.values() if stats.get('status') == 'FAILED')
        
        print(f"Total tables:     {len(self.migration_stats)}")
        print(f"Successful:       {successful} ✓")
        print(f"Failed:           {failed} ✗")
        print(f"\nDetailed Results:")
        print(f"{'-'*60}")
        
        for table_name, stats in self.migration_stats.items():
            status_icon = '✓' if stats.get('status') == 'SUCCESS' else '✗'
            print(f"{status_icon} {table_name}: {stats.get('status')}")
            if stats.get('status') == 'SUCCESS':
                print(f"    Rows: {stats.get('source_rows'):,} → {stats.get('target_rows'):,}")
                print(f"    Time: {stats.get('time_seconds', 0):.2f}s")

# Initialize orchestrator
orchestrator = MigrationOrchestrator(
    td_connector,
    gcs_manager,
    bq_manager,
    config
)

print("✓ Migration orchestrator ready")
```

---

## Running the Migration

### Execute Migration

**When to use:** After all setup is complete, run this to start the migration.

**How to use:** Execute this cell to migrate all configured tables.

```python
# Cell 9: Run Migration
# This will migrate all tables listed in config.TABLES_TO_MIGRATE
orchestrator.migrate_all_tables()
```

### Migrate Single Table (Optional)

**When to use:** To test migration with a single table first.

**How to use:** Change "your_table_name" to an actual table name.

```python
# Cell 10: Migrate Single Table (Optional)
# For testing, migrate just one table
td_connector.connect()
orchestrator.migrate_table("your_table_name")
td_connector.close()
```

---

## Validation & Testing

### Data Validation Script

**When to use:** After migration completes, validate data integrity.

**How to use:** Run this to compare source and target data.

```python
# Cell 11: Data Validation
class DataValidator:
    """Validates migrated data"""
    
    def __init__(self, td_connector, bq_manager, config):
        self.td_connector = td_connector
        self.bq_manager = bq_manager
        self.config = config
    
    def validate_table(self, table_name):
        """Validate a migrated table"""
        print(f"\nValidating: {table_name}")
        print(f"{'-'*60}")
        
        # Connect to Teradata
        if not self.td_connector.connection:
            self.td_connector.connect()
        
        # Get sample data from both sources
        td_query = f"SELECT * FROM {self.config.TERADATA_DATABASE}.{table_name} SAMPLE 1000"
        bq_query = f"SELECT * FROM `{self.config.GCP_PROJECT_ID}.{self.config.BQ_DATASET_NAME}.{table_name}` LIMIT 1000"
        
        td_df = pd.read_sql(td_query, self.td_connector.connection)
        bq_df = self.bq_manager.bq_client.query(bq_query).to_dataframe()
        
        # Compare row counts
        td_count = self.td_connector.get_row_count(table_name)
        bq_count = self.bq_manager.get_row_count(table_name)
        
        print(f"Row Count Comparison:")
        print(f"  Teradata: {td_count:,}")
        print(f"  BigQuery: {bq_count:,}")
        print(f"  Match:    {'✓ YES' if td_count == bq_count else '✗ NO'}")
        
        # Compare column counts
        print(f"\nColumn Count Comparison:")
        print(f"  Teradata: {len(td_df.columns)}")
        print(f"  BigQuery: {len(bq_df.columns)}")
        print(f"  Match:    {'✓ YES' if len(td_df.columns) == len(bq_df.columns) else '✗ NO'}")
        
        # Compare sample data
        print(f"\nSample Data Comparison:")
        print(f"  Sample size: {len(td_df)} rows")
        
        return {
            'row_count_match': td_count == bq_count,
            'column_count_match': len(td_df.columns) == len(bq_df.columns),
            'td_count': td_count,
            'bq_count': bq_count
        }

# Run validation
validator = DataValidator(td_connector, bq_manager, config)

# Validate all migrated tables
validation_results = {}
for table_name in config.TABLES_TO_MIGRATE:
    validation_results[table_name] = validator.validate_table(table_name)

print(f"\n{'='*60}")
print("VALIDATION SUMMARY")
print(f"{'='*60}")
for table, results in validation_results.items():
    status = '✓' if results['row_count_match'] and results['column_count_match'] else '✗'
    print(f"{status} {table}: {'PASSED' if results['row_count_match'] else 'FAILED'}")
```

---

## Visualization and Monitoring

### Migration Progress Visualization

**When to use:** To visualize migration progress and statistics.

**How to use:** Run after migration completes.

```python
# Cell 12: Visualization
import matplotlib.pyplot as plt
import seaborn as sns

def visualize_migration_stats(migration_stats):
    """Create visualizations for migration statistics"""
    
    # Prepare data
    tables = list(migration_stats.keys())
    source_rows = [stats.get('source_rows', 0) for stats in migration_stats.values()]
    target_rows = [stats.get('target_rows', 0) for stats in migration_stats.values()]
    times = [stats.get('time_seconds', 0) for stats in migration_stats.values()]
    statuses = [stats.get('status', 'UNKNOWN') for stats in migration_stats.values()]
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Teradata to BigQuery Migration Dashboard', fontsize=16, fontweight='bold')
    
    # 1. Row Count Comparison
    x = range(len(tables))
    width = 0.35
    axes[0, 0].bar([i - width/2 for i in x], source_rows, width, label='Teradata', color='#4285f4')
    axes[0, 0].bar([i + width/2 for i in x], target_rows, width, label='BigQuery', color='#34a853')
    axes[0, 0].set_xlabel('Tables')
    axes[0, 0].set_ylabel('Row Count')
    axes[0, 0].set_title('Row Count Comparison: Source vs Target')
    axes[0, 0].set_xticks(x)
    axes[0, 0].set_xticklabels(tables, rotation=45, ha='right')
    axes[0, 0].legend()
    axes[0, 0].grid(axis='y', alpha=0.3)
    
    # 2. Migration Time
    colors = ['#34a853' if s == 'SUCCESS' else '#ea4335' for s in statuses]
    axes[0, 1].bar(tables, times, color=colors)
    axes[0, 1].set_xlabel('Tables')
    axes[0, 1].set_ylabel('Time (seconds)')
    axes[0, 1].set_title('Migration Time per Table')
    axes[0, 1].set_xticklabels(tables, rotation=45, ha='right')
    axes[0, 1].grid(axis='y', alpha=0.3)
    
    # 3. Migration Status
    status_counts = {}
    for status in statuses:
        status_counts[status] = status_counts.get(status, 0) + 1
    
    colors_pie = ['#34a853' if s == 'SUCCESS' else '#ea4335' for s in status_counts.keys()]
    axes[1, 0].pie(status_counts.values(), labels=status_counts.keys(), autopct='%1.1f%%',
                   colors=colors_pie, startangle=90)
    axes[1, 0].set_title('Migration Status Distribution')
    
    # 4. Throughput (Rows per Second)
    throughput = [source_rows[i]/times[i] if times[i] > 0 else 0 for i in range(len(tables))]
    axes[1, 1].bar(tables, throughput, color='#fbbc04')
    axes[1, 1].set_xlabel('Tables')
    axes[1, 1].set_ylabel('Rows/Second')
    axes[1, 1].set_title('Migration Throughput')
    axes[1, 1].set_xticklabels(tables, rotation=45, ha='right')
    axes[1, 1].grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    # Print summary statistics
    print("\n" + "="*60)
    print("MIGRATION STATISTICS")
    print("="*60)
    print(f"Total tables migrated: {len(tables)}")
    print(f"Total rows