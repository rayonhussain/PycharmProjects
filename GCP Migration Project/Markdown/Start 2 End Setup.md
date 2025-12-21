# Complete GCP Data Engineering Setup Guide

## Table of Contents
1. [Initial Setup & Prerequisites](#1-initial-setup--prerequisites)
2. [GCP Project Setup](#2-gcp-project-setup)
3. [Local Development Environment](#3-local-development-environment)
4. [Git & GitHub Workflow](#4-git--github-workflow)
5. [Python Dependencies for GCP Data Engineering](#5-python-dependencies-for-gcp-data-engineering)
6. [Accessing BigQuery](#6-accessing-bigquery)
7. [Working with GCS (Google Cloud Storage)](#7-working-with-gcs)
8. [Data Engineering Best Practices](#8-data-engineering-best-practices)
9. [CI/CD with Jenkins](#9-cicd-with-jenkins)
10. [Performance Optimization](#10-performance-optimization)

---

## 1. Initial Setup & Prerequisites

### What You Need to Start
- **Google Cloud Account**: Sign up at [cloud.google.com](https://cloud.google.com)
- **Billing Account**: Set up billing (free tier available with $300 credits)
- **IDE**: VSCode or PyCharm installed
- **Git**: Version control system
- **Python**: Version 3.8 or higher

### Install Required Tools

#### Windows
```bash
# Install Python (Download from python.org)
# Install Git
winget install Git.Git

# Install Google Cloud SDK
# Download from: https://cloud.google.com/sdk/docs/install
```

#### macOS
```bash
# Install Homebrew first
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python
brew install python@3.11

# Install Git
brew install git

# Install Google Cloud SDK
brew install --cask google-cloud-sdk
```

#### Linux (Ubuntu/Debian)
```bash
# Update package list
sudo apt update

# Install Python
sudo apt install python3.11 python3-pip python3-venv

# Install Git
sudo apt install git

# Install Google Cloud SDK
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
```

---

## 2. GCP Project Setup

### Step 1: Create a GCP Project

```bash
# Login to GCP
gcloud auth login

# Create a new project
gcloud projects create my-de-project-123 --name="My Data Engineering Project"

# Set the project as default
gcloud config set project my-de-project-123

# Check current project
gcloud config get-value project
```

**Explanation**: A GCP project is like a container that holds all your cloud resources (databases, storage, etc.). Each project has a unique ID.

### Step 2: Enable Required APIs

```bash
# Enable BigQuery API
gcloud services enable bigquery.googleapis.com

# Enable Cloud Storage API
gcloud services enable storage-api.googleapis.com

# Enable Dataflow API (for data pipelines)
gcloud services enable dataflow.googleapis.com

# Enable Cloud Build API (for CI/CD)
gcloud services enable cloudbuild.googleapis.com

# View all enabled services
gcloud services list --enabled
```

### Step 3: Create Service Account

**Service Account**: Think of it as a robot user that your applications use to access GCP services securely.

```bash
# Create service account
gcloud iam service-accounts create my-de-service-account \
    --display-name="Data Engineering Service Account"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding my-de-project-123 \
    --member="serviceAccount:my-de-service-account@my-de-project-123.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

# Grant Storage permissions
gcloud projects add-iam-policy-binding my-de-project-123 \
    --member="serviceAccount:my-de-service-account@my-de-project-123.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download credentials JSON file
gcloud iam service-accounts keys create ~/gcp-key.json \
    --iam-account=my-de-service-account@my-de-project-123.iam.gserviceaccount.com
```

### Step 4: Set Environment Variable

```bash
# Linux/macOS - Add to ~/.bashrc or ~/.zshrc
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/gcp-key.json"
source ~/.bashrc

# Windows (PowerShell)
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\gcp-key.json"

# Windows (Command Prompt)
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\gcp-key.json
```

---

## 3. Local Development Environment

### Project Structure

```
my-data-project/
│
├── .git/                       # Git repository
├── .gitignore                  # Files to ignore in Git
├── venv/                       # Virtual environment (don't commit)
├── config/
│   ├── dev.yaml               # Development config
│   ├── prod.yaml              # Production config
│   └── credentials.json       # GCP credentials (don't commit)
├── src/
│   ├── __init__.py
│   ├── bigquery_operations.py
│   ├── gcs_operations.py
│   └── data_pipeline.py
├── tests/
│   ├── __init__.py
│   └── test_pipeline.py
├── sql/
│   └── queries.sql
├── requirements.txt           # Python dependencies
├── setup.py                   # Package setup
├── Dockerfile                 # For containerization
├── Jenkinsfile               # CI/CD pipeline
└── README.md                 # Project documentation
```

### Create Virtual Environment

**Virtual Environment**: An isolated Python environment for your project, preventing conflicts between different projects' dependencies.

```bash
# Navigate to your project directory
cd ~/projects/my-data-project

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# Linux/macOS
source venv/bin/activate

# Windows
venv\Scripts\activate

# Your prompt should now show (venv)
```

### Deactivate Virtual Environment
```bash
deactivate
```

---

## 4. Git & GitHub Workflow

### Initial Git Setup

```bash
# Configure Git (first time only)
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"

# Check configuration
git config --list
```

### Initialize Repository

```bash
# Initialize Git repository
git init

# Create .gitignore file
cat > .gitignore << EOF
# Virtual Environment
venv/
env/
.venv/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python

# GCP Credentials (IMPORTANT!)
*.json
config/credentials.json
gcp-key.json

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
*.log

# Environment variables
.env
.env.local
EOF
```

### GitHub Remote Repository Setup

```bash
# Create repository on GitHub first (via web interface)
# Then connect local repo to GitHub

# Add remote repository
git remote add origin https://github.com/yourusername/my-data-project.git

# Verify remote
git remote -v
```

### Branching Strategy for Development

**Branch**: A separate version of your code where you can make changes without affecting the main code.

```
main (production)
│
├── develop (integration branch)
│   │
│   ├── feature/bigquery-integration
│   ├── feature/gcs-upload
│   ├── bugfix/data-validation
│   └── hotfix/critical-bug
```

### Git Commands Workflow

```bash
# Check current branch
git branch

# Create and switch to new branch
git checkout -b feature/bigquery-integration

# Or using newer syntax
git switch -c feature/bigquery-integration

# View all branches
git branch -a

# Make changes to files, then:

# Check status
git status

# Stage specific files
git add src/bigquery_operations.py

# Stage all files
git add .

# Commit changes
git commit -m "Add BigQuery connection functionality"

# Push to GitHub
git push origin feature/bigquery-integration

# Switch back to develop branch
git checkout develop

# Pull latest changes
git pull origin develop

# Merge feature branch into develop
git merge feature/bigquery-integration

# Delete local branch after merge
git branch -d feature/bigquery-integration

# Delete remote branch
git push origin --delete feature/bigquery-integration
```

### Pull Request Workflow

```bash
# 1. Create feature branch
git checkout develop
git pull origin develop
git checkout -b feature/new-pipeline

# 2. Make changes and commit
git add .
git commit -m "Implement new data pipeline"

# 3. Push to GitHub
git push origin feature/new-pipeline

# 4. Go to GitHub and create Pull Request
#    - Base: develop
#    - Compare: feature/new-pipeline
#    - Request review from team members

# 5. After approval and merge on GitHub
git checkout develop
git pull origin develop

# 6. Delete local feature branch
git branch -d feature/new-pipeline
```

### Common Git Scenarios

```bash
# Undo last commit (keep changes)
git reset --soft HEAD~1

# Discard all local changes
git reset --hard HEAD

# View commit history
git log --oneline --graph --all

# Stash changes temporarily
git stash
git stash list
git stash apply

# Cherry-pick specific commit
git cherry-pick <commit-hash>

# Rebase feature branch on develop
git checkout feature/my-feature
git rebase develop
```

---

## 5. Python Dependencies for GCP Data Engineering

### Core Requirements File

Create `requirements.txt`:

```txt
# GCP Core Libraries
google-cloud-bigquery==3.14.0
google-cloud-storage==2.14.0
google-cloud-dataflow==2.52.0
google-cloud-pubsub==2.19.0
google-cloud-secret-manager==2.17.0

# Authentication
google-auth==2.25.2
google-auth-oauthlib==1.2.0
google-auth-httplib2==0.2.0

# Data Processing
pandas==2.1.4
numpy==1.26.2
pyarrow==14.0.2
fastparquet==2023.10.1

# Database Connectors
sqlalchemy==2.0.25
psycopg2-binary==2.9.9
pymysql==1.1.0

# Data Validation
great-expectations==0.18.8
pydantic==2.5.3

# Workflow Orchestration
apache-airflow==2.8.0
prefect==2.14.9

# Testing
pytest==7.4.3
pytest-cov==4.1.0
mock==5.1.0

# Logging & Monitoring
python-json-logger==2.0.7
opencensus-ext-stackdriver==0.11.0

# Utilities
python-dotenv==1.0.0
pyyaml==6.0.1
requests==2.31.0
tqdm==4.66.1

# Code Quality
black==23.12.1
flake8==7.0.0
pylint==3.0.3
```

### Install Dependencies

```bash
# Activate virtual environment first
source venv/bin/activate

# Install all dependencies
pip install -r requirements.txt

# Or install individually
pip install google-cloud-bigquery google-cloud-storage pandas

# Upgrade pip first (recommended)
pip install --upgrade pip

# Freeze current environment
pip freeze > requirements-lock.txt
```

### Dependency Explanations

| Library | Purpose | When to Use |
|---------|---------|-------------|
| `google-cloud-bigquery` | Interact with BigQuery | Querying, loading data to BigQuery |
| `google-cloud-storage` | Work with Cloud Storage | Upload/download files to GCS buckets |
| `pandas` | Data manipulation | Transform, clean, analyze data |
| `pyarrow` | Fast columnar format | Read/write Parquet files efficiently |
| `fastparquet` | Parquet file handling | Alternative Parquet implementation |
| `sqlalchemy` | Database abstraction | Connect to various databases |
| `great-expectations` | Data validation | Ensure data quality and consistency |
| `apache-airflow` | Workflow orchestration | Schedule and monitor data pipelines |

---

## 6. Accessing BigQuery

### BigQuery Basics

**BigQuery**: Google's serverless data warehouse. Think of it as a giant Excel spreadsheet in the cloud that can handle massive amounts of data.

**Key Concepts**:
- **Project**: Your GCP project
- **Dataset**: Like a database or folder that contains tables
- **Table**: Where your data lives
- **Schema**: Definition of columns and their data types

### Using gcloud CLI

```bash
# List all datasets
bq ls

# Create a dataset
bq mk --dataset my-de-project-123:my_dataset

# List tables in a dataset
bq ls my-de-project-123:my_dataset

# Show table schema
bq show --schema --format=prettyjson my-de-project-123:my_dataset.my_table

# Run a query
bq query --use_legacy_sql=false \
'SELECT 
  name, 
  COUNT(*) as count 
FROM `my-de-project-123.my_dataset.my_table` 
GROUP BY name 
LIMIT 10'

# Load data from GCS to BigQuery
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  my_dataset.my_table \
  gs://my-bucket/data.csv \
  name:STRING,age:INTEGER,city:STRING

# Export BigQuery table to GCS
bq extract \
  --destination_format=PARQUET \
  my_dataset.my_table \
  gs://my-bucket/output/*.parquet
```

### Python BigQuery Client

Create `src/bigquery_operations.py`:

```python
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BigQueryManager:
    """Manage BigQuery operations"""
    
    def __init__(self, project_id: str):
        """
        Initialize BigQuery client
        
        Args:
            project_id: GCP project ID
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        logger.info(f"Initialized BigQuery client for project: {project_id}")
    
    def create_dataset(self, dataset_id: str, location: str = "US") -> None:
        """
        Create a BigQuery dataset
        
        Args:
            dataset_id: Name of the dataset
            location: Geographic location (US, EU, asia-south1, etc.)
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset = self.client.create_dataset(dataset)
            logger.info(f"Created dataset {dataset_id} in {location}")
    
    def run_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame
        
        Args:
            query: SQL query string
            
        Returns:
            Pandas DataFrame with query results
        """
        logger.info("Executing query...")
        query_job = self.client.query(query)
        results = query_job.result()
        df = results.to_dataframe()
        logger.info(f"Query returned {len(df)} rows")
        return df
    
    def load_csv_to_table(
        self, 
        dataset_id: str, 
        table_id: str, 
        gcs_uri: str,
        skip_leading_rows: int = 1,
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> None:
        """
        Load CSV from GCS to BigQuery table
        
        Args:
            dataset_id: Dataset name
            table_id: Table name
            gcs_uri: GCS path (gs://bucket/file.csv)
            skip_leading_rows: Number of header rows to skip
            write_disposition: WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=skip_leading_rows,
            autodetect=True,  # Auto-detect schema
            write_disposition=write_disposition,
        )
        
        load_job = self.client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )
        
        load_job.result()  # Wait for job to complete
        
        table = self.client.get_table(table_ref)
        logger.info(f"Loaded {table.num_rows} rows to {table_ref}")
    
    def load_dataframe_to_table(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> None:
        """
        Load Pandas DataFrame to BigQuery table
        
        Args:
            df: Pandas DataFrame
            dataset_id: Dataset name
            table_id: Table name
            write_disposition: How to write data
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
        )
        
        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        
        job.result()
        logger.info(f"Loaded {len(df)} rows to {table_ref}")
    
    def create_partitioned_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: List[bigquery.SchemaField],
        partition_field: str,
        partition_type: str = "DAY"
    ) -> None:
        """
        Create a partitioned table for better performance
        
        Args:
            dataset_id: Dataset name
            table_id: Table name
            schema: Table schema
            partition_field: Field to partition by (must be DATE or TIMESTAMP)
            partition_type: DAY, HOUR, MONTH, or YEAR
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        table = bigquery.Table(table_ref, schema=schema)
        
        # Set partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=getattr(bigquery.TimePartitioningType, partition_type),
            field=partition_field,
        )
        
        table = self.client.create_table(table)
        logger.info(f"Created partitioned table {table_ref}")
    
    def export_to_gcs(
        self,
        dataset_id: str,
        table_id: str,
        gcs_uri: str,
        export_format: str = "PARQUET"
    ) -> None:
        """
        Export BigQuery table to GCS
        
        Args:
            dataset_id: Dataset name
            table_id: Table name
            gcs_uri: Destination GCS path (gs://bucket/path/*.parquet)
            export_format: CSV, JSON, AVRO, or PARQUET
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = getattr(
            bigquery.DestinationFormat, export_format
        )
        
        extract_job = self.client.extract_table(
            table_ref,
            gcs_uri,
            job_config=job_config,
        )
        
        extract_job.result()
        logger.info(f"Exported {table_ref} to {gcs_uri}")


# Example Usage
if __name__ == "__main__":
    # Initialize
    bq = BigQueryManager(project_id="my-de-project-123")
    
    # Create dataset
    bq.create_dataset("analytics", location="US")
    
    # Run query
    query = """
    SELECT 
        user_id,
        COUNT(*) as event_count,
        MAX(event_date) as last_event
    FROM `my-de-project-123.analytics.events`
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY user_id
    ORDER BY event_count DESC
    LIMIT 100
    """
    df = bq.run_query(query)
    print(df.head())
    
    # Load CSV from GCS
    bq.load_csv_to_table(
        dataset_id="analytics",
        table_id="users",
        gcs_uri="gs://my-bucket/users.csv"
    )
    
    # Load DataFrame
    sample_df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'city': ['NYC', 'LA', 'Chicago']
    })
    bq.load_dataframe_to_table(sample_df, "analytics", "sample_table")
```

### BigQuery SQL Examples

```sql
-- Create a new table from query results
CREATE OR REPLACE TABLE `my-de-project-123.analytics.user_summary` AS
SELECT 
  user_id,
  COUNT(*) as total_events,
  MIN(event_date) as first_seen,
  MAX(event_date) as last_seen,
  COUNTIF(event_type = 'purchase') as purchases
FROM `my-de-project-123.analytics.events`
GROUP BY user_id;

-- Partitioned table creation
CREATE OR REPLACE TABLE `my-de-project-123.analytics.events_partitioned`
PARTITION BY DATE(event_timestamp)
CLUSTER BY user_id, event_type
AS SELECT * FROM `my-de-project-123.analytics.events`;

-- Insert data
INSERT INTO `my-de-project-123.analytics.events` (user_id, event_type, event_date)
VALUES 
  (1001, 'page_view', CURRENT_DATE()),
  (1002, 'purchase', CURRENT_DATE());

-- Update data
UPDATE `my-de-project-123.analytics.users`
SET status = 'active'
WHERE last_login >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

-- Delete old data
DELETE FROM `my-de-project-123.analytics.events`
WHERE event_date < DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY);

-- Window functions for analytics
SELECT 
  user_id,
  event_date,
  event_type,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date) as event_sequence,
  LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date) as previous_event_date,
  DATE_DIFF(event_date, LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date), DAY) as days_since_last_event
FROM `my-de-project-123.analytics.events`;
```

---

## 7. Working with GCS (Google Cloud Storage)

### GCS Basics

**Cloud Storage**: Like Google Drive for your applications. Used to store files (CSV, JSON, Parquet, images, etc.)

**Key Concepts**:
- **Bucket**: Container for your files (must be globally unique name)
- **Object**: A file in your bucket
- **Storage Class**: How frequently you access data (affects cost)

### Using gcloud CLI

```bash
# Create a bucket
gsutil mb -l us-central1 -c STANDARD gs://my-de-bucket-unique-name

# List buckets
gsutil ls

# Upload file
gsutil cp local-file.csv gs://my-de-bucket-unique-name/

# Upload folder recursively
gsutil -m cp -r ./data_folder gs://my-de-bucket-unique-name/data/

# Download file
gsutil cp gs://my-de-bucket-unique-name/file.csv ./local-file.csv

# List objects in bucket
gsutil ls gs://my-de-bucket-unique-name/

# List with details
gsutil ls -l gs://my-de-bucket-unique-name/

# Delete file
gsutil rm gs://my-de-bucket-unique-name/file.csv

# Delete bucket (must be empty)
gsutil rb gs://my-de-bucket-unique-name

# Make file publicly accessible
gsutil acl ch -u AllUsers:R gs://my-de-bucket-unique-name/public-file.csv

# Sync local folder with bucket
gsutil -m rsync -r ./local_folder gs://my-de-bucket-unique-name/folder/
```

### Python GCS Client

Create `src/gcs_operations.py`:

```python
from google.cloud import storage
from google.cloud.exceptions import NotFound
import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GCSManager:
    """Manage Google Cloud Storage operations"""
    
    def __init__(self, project_id: str):
        """
        Initialize GCS client
        
        Args:
            project_id: GCP project ID
        """
        self.client = storage.Client(project=project_id)
        self.project_id = project_id
        logger.info(f"Initialized GCS client for project: {project_id}")
    
    def create_bucket(
        self, 
        bucket_name: str, 
        location: str = "US",
        storage_class: str = "STANDARD"
    ) -> storage.Bucket:
        """
        Create a GCS bucket
        
        Args:
            bucket_name: Unique bucket name
            location: Geographic location
            storage_class: STANDARD, NEARLINE, COLDLINE, or ARCHIVE
            
        Returns:
            Created bucket object
        """
        try:
            bucket = self.client.get_bucket(bucket_name)
            logger.info(f"Bucket {bucket_name} already exists")
            return bucket
        except NotFound:
            bucket = self.client.bucket(bucket_name)
            bucket.storage_class = storage_class
            bucket = self.client.create_bucket(bucket, location=location)
            logger.info(f"Created bucket {bucket_name} in {location}")
            return bucket
    
    def upload_file(
        self, 
        bucket_name: str, 
        source_file_path: str, 
        destination_blob_name: str
    ) -> None:
        """
        Upload a file to GCS
        
        Args:
            bucket_name: Target bucket name
            source_file_path: Local file path
            destination_blob_name: Path in bucket (e.g., 'folder/file.csv')
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_filename(source_file_path)
        
        logger.info(f"Uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}")
    
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        bucket_name: str,
        destination_blob_name: str,
        file_format: str = "csv"
    ) -> None:
        """
        Upload Pandas DataFrame to GCS
        
        Args:
            df: Pandas DataFrame
            bucket_name: Target bucket name
            destination_blob_name: Path in bucket
            file_format: 'csv', 'parquet', or 'json'
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Convert DataFrame to bytes
        if file_format == "csv":
            data = df.to_csv(index=False).encode()
        elif file_format == "parquet":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            data = buffer.getvalue()
        elif file_format == "json":
            data = df.to_json(orient='records').encode()
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        blob.upload_from_string(data)
        logger.info(f"Uploaded DataFrame to gs://{bucket_name}/{destination_blob_name}")
    
    def download_file(
        self, 
        bucket_name: str, 
        source_blob_name: str, 
        destination_file_path: str
    ) -> None:
        """
        Download a file from GCS
        
        Args:
            bucket_name: Source bucket name
            source_blob_name: Path in bucket
            destination_file_path: Local destination path
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        
        blob.download_to_filename(destination_file_path)
        
        logger.info(f"Downloaded gs://{bucket_name}/{source_blob_name} to {destination_file_path}")
    
    def read_to_dataframe(
        self,
        bucket_name: str,
        blob_name: str,
        file_format: str = "csv"
    ) -> pd.DataFrame:
        """
        Read file from GCS directly to DataFrame
        
        Args:
            bucket_name: Source bucket name
            blob_name: Path in bucket
            file_format: 'csv', 'parquet', or 'json'
            
        Returns:
            Pandas DataFrame
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        data = blob.download_as_bytes()
        
        if file_format == "csv":
            df = pd.read_csv(io.BytesIO(data))
        elif file_format == "parquet":
            df = pd.read_parquet(io.BytesIO(data))
        elif file_format == "json":
            df = pd.read_json(io.BytesIO(data))
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        logger.info(f"Read {len(df)} rows from gs://{bucket_name}/{blob_name}")
        return df
    
    def list_blobs(
        self, 
        bucket_name: str, 
        prefix: Optional[str] = None
    ) -> List[str]:
        """
        List all files in a bucket
        
        Args:
            bucket_name: Bucket name
            prefix: Filter by prefix (folder path)
            
        Returns:
            List of blob names
        """
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        
        blob_names = [blob.name for blob in blobs]
        logger.info(f"Found {len(blob_names)} objects in gs://{bucket_name}/{prefix or ''}")
        return blob_names
    
    def delete_blob(self, bucket_name: str, blob_name: str) -> None:
        """
        Delete a file from GCS
        
        Args:
            bucket_name: Bucket name
            blob_name: Path in bucket
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        
        logger.info(f"Deleted gs://{bucket_name}/{blob_name}")
    
    def copy_blob(
        self,
        source_bucket_name: str,
        source_blob_name: str,
        destination_bucket_name: str,
        destination_blob_name: str
    ) -> None:
        """
        Copy a blob from one location to another
        
        Args:
            source_bucket_name: Source bucket
            source_blob_name: Source path
            destination_bucket_name: Destination bucket
            destination_blob_name: Destination path
        """
        source_bucket = self.client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)
        destination_bucket = self.client.bucket(destination_bucket_name)
        
        source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )
        
        logger.info(f"Copied gs://{source_bucket_name}/{source_blob_name} to gs://{destination_bucket_name}/{destination_blob_name}")


# Example Usage
if __name__ == "__main__":
    gcs = GCSManager(project_id="my-de-project-123")
    
    # Create bucket
    gcs.create_bucket("my-de-bucket-12345", location="US")
    
    # Upload file
    gcs.upload_file("my-de-bucket-12345", "data.csv", "raw/data.csv")
    
    # Upload DataFrame
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    gcs.upload_dataframe(df, "my-de-bucket-12345", "processed/data.parquet", "parquet")
    
    # List files
    files = gcs.list_blobs("my-de-bucket-12345", prefix="raw/")
    print(files)
    
    # Read to DataFrame
    df = gcs.read_to_dataframe("my-de-bucket-12345", "processed/data.parquet", "parquet")
    print(df)
```

---

## 8. Data Engineering Best Practices

### File Formats Comparison

| Format | Use Case | Pros | Cons | Compression |
|--------|----------|------|------|-------------|
| **CSV** | Simple data exchange | Human-readable, universal | Large size, no schema, slow | GZIP |
| **JSON** | Nested data, APIs | Flexible schema, human-readable | Large size, slow parsing | GZIP |
| **Parquet** | Analytics, BigQuery | Columnar, compressed, fast | Not human-readable | Built-in |
| **Avro** | Streaming, schema evolution | Row-based, schema included | Less efficient for analytics | Built-in |
| **ORC** | Hive, large datasets | Highly compressed | Hadoop ecosystem only | Built-in |

**Recommendation**: Use **Parquet** for most data engineering tasks in GCP.

### Partitioning Strategies

**Partitioning**: Dividing large tables into smaller segments for faster queries and lower costs.

```python
# Example: Partitioned table structure in BigQuery
"""
Table: events_partitioned

Partitioned by: event_date (DATE)
Clustered by: user_id, event_type

Data organization:
├── partition: 2024-01-01
│   ├── cluster: user_id=1001
│   └── cluster: user_id=1002
├── partition: 2024-01-02
│   ├── cluster: user_id=1001
│   └── cluster: user_id=1003
"""

# Create partitioned table
from google.cloud import bigquery

schema = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("user_id", "INTEGER"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("event_date", "DATE"),
]

table = bigquery.Table("project.dataset.events", schema=schema)

# Partition by date
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="event_date",
)

# Cluster for faster queries
table.clustering_fields = ["user_id", "event_type"]

client.create_table(table)
```

**Benefits**:
- Queries only scan relevant partitions (cost savings)
- Faster query performance
- Easier data lifecycle management (delete old partitions)

### Handling Different Core Sizes & Optimization

```python
# Optimize DataFrame processing based on data size

import pandas as pd
from multiprocessing import cpu_count

def process_large_file(file_path: str, chunk_size: int = 10000):
    """
    Process large CSV in chunks to avoid memory issues
    
    Args:
        file_path: Path to CSV file
        chunk_size: Number of rows per chunk
    """
    # Read in chunks
    chunks = pd.read_csv(file_path, chunksize=chunk_size)
    
    results = []
    for chunk in chunks:
        # Process each chunk
        processed = chunk.groupby('category')['value'].sum()
        results.append(processed)
    
    # Combine results
    final_result = pd.concat(results).groupby(level=0).sum()
    return final_result


# Parallel processing with multiprocessing
from concurrent.futures import ProcessPoolExecutor

def process_single_file(file_path: str) -> pd.DataFrame:
    """Process a single file"""
    df = pd.read_csv(file_path)
    # Do transformations
    return df

def process_multiple_files_parallel(file_paths: list) -> pd.DataFrame:
    """
    Process multiple files in parallel
    
    Args:
        file_paths: List of file paths
        
    Returns:
        Combined DataFrame
    """
    num_cores = cpu_count()
    
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        results = executor.map(process_single_file, file_paths)
    
    # Combine all results
    combined_df = pd.concat(results, ignore_index=True)
    return combined_df


# Using Dask for distributed computing (beyond pandas)
import dask.dataframe as dd

def process_with_dask(file_pattern: str):
    """
    Process very large datasets with Dask
    
    Args:
        file_pattern: Pattern to match files (e.g., 'data/*.csv')
    """
    # Read multiple files as one large DataFrame
    ddf = dd.read_csv(file_pattern)
    
    # Perform operations (lazy evaluation)
    result = ddf.groupby('category')['value'].mean()
    
    # Compute result (actual execution)
    final = result.compute()
    return final
```

### Storage Class Selection

```python
# GCS Storage Classes and when to use them

from google.cloud import storage

def set_storage_class_based_on_access_pattern(
    bucket_name: str,
    blob_name: str,
    access_pattern: str
):
    """
    Set storage class based on access frequency
    
    Args:
        bucket_name: Bucket name
        blob_name: Object name
        access_pattern: 'hot', 'warm', 'cold', 'archive'
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Map access patterns to storage classes
    storage_classes = {
        'hot': 'STANDARD',      # Frequent access (daily)
        'warm': 'NEARLINE',     # Monthly access, 30-day minimum
        'cold': 'COLDLINE',     # Quarterly access, 90-day minimum
        'archive': 'ARCHIVE'    # Yearly access, 365-day minimum
    }
    
    blob.update_storage_class(storage_classes[access_pattern])
    
    print(f"Updated {blob_name} to {storage_classes[access_pattern]}")

# Lifecycle management (auto-transition old data)
def setup_lifecycle_policy(bucket_name: str):
    """
    Setup automatic lifecycle management
    
    This automatically moves data to cheaper storage as it ages
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Define lifecycle rules
    rules = [
        {
            "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
            "condition": {"age": 30}  # Move to NEARLINE after 30 days
        },
        {
            "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
            "condition": {"age": 90}  # Move to COLDLINE after 90 days
        },
        {
            "action": {"type": "Delete"},
            "condition": {"age": 365}  # Delete after 1 year
        }
    ]
    
    bucket.lifecycle_rules = rules
    bucket.patch()
    
    print(f"Lifecycle policy set for {bucket_name}")
```

### Data Pipeline Example

Create `src/data_pipeline.py`:

```python
import pandas as pd
from datetime import datetime
import logging
from bigquery_operations import BigQueryManager
from gcs_operations import GCSManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataPipeline:
    """Complete data pipeline: Extract -> Transform -> Load"""
    
    def __init__(self, project_id: str, bucket_name: str):
        self.bq = BigQueryManager(project_id)
        self.gcs = GCSManager(project_id)
        self.bucket_name = bucket_name
        self.project_id = project_id
    
    def extract(self, source_path: str) -> pd.DataFrame:
        """
        Extract data from source
        
        Args:
            source_path: GCS path or local path
            
        Returns:
            Raw DataFrame
        """
        logger.info(f"Extracting data from {source_path}")
        
        if source_path.startswith('gs://'):
            # Extract from GCS
            parts = source_path.replace('gs://', '').split('/', 1)
            bucket = parts[0]
            blob = parts[1]
            df = self.gcs.read_to_dataframe(bucket, blob, 'csv')
        else:
            # Extract from local file
            df = pd.read_csv(source_path)
        
        logger.info(f"Extracted {len(df)} rows")
        return df
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data: clean, validate, enrich
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming data...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        df = df.dropna(subset=['user_id'])  # Drop rows without user_id
        df['age'] = df['age'].fillna(df['age'].median())  # Fill age with median
        
        # Data type conversions
        df['user_id'] = df['user_id'].astype(int)
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Add derived columns
        df['year'] = df['created_at'].dt.year
        df['month'] = df['created_at'].dt.month
        df['is_active'] = df['last_login'] > (datetime.now() - pd.Timedelta(days=30))
        
        # Data validation
        assert df['age'].min() >= 0, "Age cannot be negative"
        assert df['user_id'].is_unique, "User IDs must be unique"
        
        logger.info(f"Transformed to {len(df)} rows")
        return df
    
    def load(
        self, 
        df: pd.DataFrame, 
        dataset_id: str, 
        table_id: str,
        save_to_gcs: bool = True
    ) -> None:
        """
        Load data to BigQuery and optionally GCS
        
        Args:
            df: Transformed DataFrame
            dataset_id: BigQuery dataset
            table_id: BigQuery table
            save_to_gcs: Also save to GCS as backup
        """
        logger.info("Loading data...")
        
        # Load to BigQuery
        self.bq.load_dataframe_to_table(
            df, 
            dataset_id, 
            table_id, 
            write_disposition='WRITE_TRUNCATE'
        )
        
        # Optionally save to GCS
        if save_to_gcs:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"processed/{table_id}_{timestamp}.parquet"
            self.gcs.upload_dataframe(
                df, 
                self.bucket_name, 
                blob_name, 
                'parquet'
            )
        
        logger.info("Data loading complete")
    
    def run(
        self, 
        source_path: str, 
        dataset_id: str, 
        table_id: str
    ) -> None:
        """
        Run complete ETL pipeline
        
        Args:
            source_path: Source data location
            dataset_id: Target dataset
            table_id: Target table
        """
        try:
            # Extract
            raw_df = self.extract(source_path)
            
            # Transform
            transformed_df = self.transform(raw_df)
            
            # Load
            self.load(transformed_df, dataset_id, table_id)
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


# Example Usage
if __name__ == "__main__":
    pipeline = DataPipeline(
        project_id="my-de-project-123",
        bucket_name="my-de-bucket-12345"
    )
    
    pipeline.run(
        source_path="gs://my-de-bucket-12345/raw/users.csv",
        dataset_id="analytics",
        table_id="users_processed"
    )
```

---

## 9. CI/CD with Jenkins

### Jenkins Setup

**Jenkins**: Automation server that runs your data pipelines automatically when code changes.

**Install Jenkins**:

```bash
# On Linux (Ubuntu)
wget -q -O - https://pkg.jenkins.io/debian-stable/jenkins.io.key | sudo apt-key add -
sudo sh -c 'echo deb https://pkg.jenkins.io/debian-stable binary/ > /etc/apt/sources.list.d/jenkins.list'
sudo apt update
sudo apt install jenkins

# Start Jenkins
sudo systemctl start jenkins
sudo systemctl enable jenkins

# Access at: http://localhost:8080
# Get initial password:
sudo cat /var/lib/jenkins/secrets/initialAdminPassword
```

### Jenkinsfile

Create `Jenkinsfile` in your project root:

```groovy
pipeline {
    agent any
    
    environment {
        PROJECT_ID = 'my-de-project-123'
        GCS_BUCKET = 'my-de-bucket-12345'
        DATASET_ID = 'analytics'
        GOOGLE_APPLICATION_CREDENTIALS = credentials('gcp-service-account')
    }
    
    stages {
        stage('Checkout') {
            steps {
                // Pull code from Git
                git branch: 'main',
                    url: 'https://github.com/yourusername/my-data-project.git',
                    credentialsId: 'github-credentials'
            }
        }
        
        stage('Setup Environment') {
            steps {
                sh '''
                    # Create virtual environment
                    python3 -m venv venv
                    source venv/bin/activate
                    
                    # Install dependencies
                    pip install --upgrade pip
                    pip install -r requirements.txt
                '''
            }
        }
        
        stage('Run Tests') {
            steps {
                sh '''
                    source venv/bin/activate
                    
                    # Run unit tests
                    pytest tests/ -v --cov=src --cov-report=xml
                    
                    # Check code quality
                    flake8 src/ --max-line-length=100
                    black --check src/
                '''
            }
        }
        
        stage('Run Data Pipeline') {
            steps {
                sh '''
                    source venv/bin/activate
                    
                    # Run the data pipeline
                    python src/data_pipeline.py
                '''
            }
        }
        
        stage('Data Quality Checks') {
            steps {
                sh '''
                    source venv/bin/activate
                    
                    # Run Great Expectations validation
                    great_expectations checkpoint run data_quality_checkpoint
                '''
            }
        }
        
        stage('Deploy to Production') {
            when {
                branch 'main'
            }
            steps {
                sh '''
                    source venv/bin/activate
                    
                    # Deploy BigQuery views/procedures
                    bq query --use_legacy_sql=false < sql/create_views.sql
                '''
            }
        }
    }
    
    post {
        success {
            echo 'Pipeline completed successfully!'
            // Send notification (Slack, email, etc.)
        }
        failure {
            echo 'Pipeline failed!'
            // Send alert
        }
        always {
            // Clean up
            cleanWs()
        }
    }
}
```

### Jenkins Pipeline Triggers

```groovy
// Trigger pipeline on Git push
pipeline {
    agent any
    
    triggers {
        // Check GitHub for changes every 5 minutes
        pollSCM('H/5 * * * *')
        
        // Or use GitHub webhook for instant triggers
        githubPush()
        
        // Schedule regular runs (cron syntax)
        cron('0 2 * * *')  // Run at 2 AM daily
    }
    
    // ... rest of pipeline
}
```

### GitHub Actions Alternative

Create `.github/workflows/ci-cd.yml`:

```yaml
name: Data Pipeline CI/CD

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: '0 2 * * *'  # Run daily at 2 AM

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run tests
      run: |
        pytest tests/ -v --cov=src
        flake8 src/
    
    - name: Authenticate to Google Cloud
      uses: google-github-actions/auth@v1
      with:
        credentials_json: ${{ secrets.GCP_SA_KEY }}
    
    - name: Set up Cloud SDK
      uses: google-github-actions/setup-gcloud@v1
    
    - name: Run data pipeline
      run: |
        python src/data_pipeline.py
    
    - name: Deploy to BigQuery
      if: github.ref == 'refs/heads/main'
      run: |
        bq query --use_legacy_sql=false < sql/create_views.sql
```

---

## 10. Performance Optimization

### Query Optimization in BigQuery

```sql
-- ❌ BAD: Full table scan
SELECT * 
FROM `project.dataset.large_table`
WHERE date = '2024-01-01';

-- ✅ GOOD: Partition pruning
SELECT user_id, event_type
FROM `project.dataset.large_table`
WHERE date = '2024-01-01'  -- Uses partition
  AND event_type IN ('purchase', 'signup');  -- Uses clustering

-- ❌ BAD: Non-clustered join
SELECT a.*, b.details
FROM `project.dataset.users` a
JOIN `project.dataset.events` b ON CAST(a.user_id AS STRING) = b.user_id_string;

-- ✅ GOOD: Clustered join with proper types
SELECT a.*, b.details
FROM `project.dataset.users` a
JOIN `project.dataset.events` b ON a.user_id = b.user_id;

-- ❌ BAD: Multiple subqueries
SELECT *
FROM (
  SELECT * FROM `project.dataset.table1`
  WHERE date > '2024-01-01'
)
WHERE status = 'active';

-- ✅ GOOD: Single query with all filters
SELECT *
FROM `project.dataset.table1`
WHERE date > '2024-01-01'
  AND status = 'active';

-- Use approximate aggregations for large datasets
SELECT 
  APPROX_COUNT_DISTINCT(user_id) as unique_users,
  APPROX_QUANTILES(value, 100)[OFFSET(50)] as median_value
FROM `project.dataset.events`;
```

### Parallel Processing Strategies

```python
# Process large datasets efficiently

from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
import pandas as pd

def process_partition(partition_date: str) -> pd.DataFrame:
    """Process a single partition"""
    client = bigquery.Client()
    
    query = f"""
    SELECT *
    FROM `project.dataset.table`
    WHERE date = '{partition_date}'
    """
    
    df = client.query(query).to_dataframe()
    # Transform data
    df_transformed = transform(df)
    return df_transformed

def parallel_partition_processing(date_list: list) -> pd.DataFrame:
    """
    Process multiple partitions in parallel
    
    Args:
        date_list: List of dates to process
        
    Returns:
        Combined DataFrame
    """
    results = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(process_partition, date): date 
            for date in date_list
        }
        
        for future in as_completed(futures):
            date = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"Completed: {date}")
            except Exception as e:
                print(f"Failed {date}: {str(e)}")
    
    return pd.concat(results, ignore_index=True)


# Usage
dates = ['2024-01-01', '2024-01-02', '2024-01-03']
final_df = parallel_partition_processing(dates)
```

### Memory Management Tips

```python
# Efficient memory usage

# 1. Use generators for large files
def read_large_csv_generator(file_path: str, chunksize: int = 10000):
    """
    Read CSV in chunks using generator
    
    Yields:
        DataFrame chunks
    """
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        yield chunk

# Usage
for chunk in read_large_csv_generator('large_file.csv'):
    # Process chunk
    process(chunk)
    # Chunk is garbage collected after this iteration


# 2. Reduce DataFrame memory usage
def reduce_memory_usage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize DataFrame memory by downcasting types
    
    Args:
        df: Input DataFrame
        
    Returns:
        Memory-optimized DataFrame
    """
    start_mem = df.memory_usage().sum() / 1024**2
    
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
    
    end_mem = df.memory_usage().sum() / 1024**2
    print(f'Memory reduced from {start_mem:.2f} MB to {end_mem:.2f} MB ({100 * (start_mem - end_mem) / start_mem:.1f}% reduction)')
    
    return df


# 3. Use categorical data types
df['category_column'] = df['category_column'].astype('category')
```

---

## Quick Reference Commands

### Essential Git Commands
```bash
git status                    # Check status
git add .                     # Stage all changes
git commit -m "message"       # Commit changes
git push origin branch-name   # Push to remote
git pull origin branch-name   # Pull from remote
git checkout -b new-branch    # Create new branch
git merge branch-name         # Merge branch
git log --oneline            # View commit history
```

### Essential GCP Commands
```bash
gcloud config list                          # Show config
gcloud projects list                        # List projects
gsutil ls                                   # List buckets
gsutil cp file gs://bucket/                # Upload to GCS
bq ls                                       # List datasets
bq show dataset.table                       # Show table info
bq query "SELECT * FROM dataset.table"     # Run query
```

### Python Virtual Environment
```bash
python -m venv venv           # Create venv
source venv/bin/activate      # Activate (Linux/Mac)
venv\Scripts\activate         # Activate (Windows)
pip install -r requirements.txt  # Install deps
pip freeze > requirements.txt    # Save deps
deactivate                    # Deactivate venv
```

---

## Visual Workflow Diagram

```
┌─────────────────┐
│  Local Dev      │
│  (VSCode/PyCharm)│
└────────┬────────┘
         │
         │ git push
         ▼
┌─────────────────┐
│  GitHub         │
│  (Version Control)│
└────────┬────────┘
         │
         │ webhook/trigger
         ▼
┌─────────────────┐
│  Jenkins/Actions│
│  (CI/CD)        │
└────────┬────────┘
         │
         │ run tests & deploy
         ▼
┌─────────────────┐       ┌─────────────────┐
│  GCS            │◄──────┤  Extract        │
│  (Storage)      │       │  Transform      │
└────────┬────────┘       └────────┬────────┘
         │                         │
         │ load data               │ processed data
         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐
│  BigQuery       │       │  Load           │
│  (Data Warehouse)│◄──────┤  (ETL Pipeline) │
└─────────────────┘       └─────────────────┘
         │
         │ analytics
         ▼
┌─────────────────┐
│  Dashboards     │
│  (Data Studio)  │
└─────────────────┘
```

---

## Additional Resources

- **GCP Documentation**: https://cloud.google.com/docs
- **BigQuery Best Practices**: https://cloud.google.com/bigquery/docs/best-practices
- **Git Tutorial**: https://git-scm.com/doc
- **Python Data Engineering**: https://docs.python.org/3/
- **Stack Overflow**: https://stackoverflow.com/questions/tagged/google-bigquery

---

## Troubleshooting Common Issues

### 1. Authentication Errors
```bash
# Reset authentication
gcloud auth revoke
gcloud auth login

# Set correct project
gcloud config set project YOUR_PROJECT_ID

# Check credentials path
echo $GOOGLE_APPLICATION_CREDENTIALS
```

### 2. Quota Exceeded
```python
# Implement exponential backoff
import time
from google.api_core.exceptions import TooManyRequests

def retry_with_backoff(func, max_retries=5):
    for attempt in range(max_retries):
        try:
            return func()
        except TooManyRequests:
            wait_time = 2 ** attempt
            print(f"Rate limited. Waiting {wait_time}s...")
            time.sleep(wait_time)
    raise Exception("Max retries exceeded")
```

### 3. Memory Issues
```python
# Process in smaller chunks
chunk_size = 1000  # Adjust based on available memory
for chunk in pd.read_csv('large.csv', chunksize=chunk_size):
    process(chunk)
```

---

## Next Steps

1. **Set up your environment** following Section 1-3
2. **Create a simple pipeline** using Section 8
3. **Set up version control** with Section 4
4. **Deploy with CI/CD** using Section 9
5. **Optimize performance** with Section 10

Good luck with your GCP Data Engineering journey!