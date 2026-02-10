import os
import logging
import json
import time
from typing import Generator, List, Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from google.cloud import bigquery
from google.auth import default

# ===========================
# 1. LOGGING CONFIGURATION
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    handlers=[
        logging.StreamHandler()  # Output to console (GKE logs)
    ]
)
logger = logging.getLogger(__name__)

# ===========================
# 2. ENVIRONMENT VARIABLES
# ===========================
# BigQuery Config
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "your-project-id")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "staging")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "raw_data")

# Cloud SQL Config (for GKE with Cloud SQL Proxy sidecar)
CLOUD_SQL_HOST = os.getenv("CLOUD_SQL_HOST", "127.0.0.1")  # Usually 127.0.0.1 in GKE
CLOUD_SQL_PORT = os.getenv("CLOUD_SQL_PORT", "3306")
CLOUD_SQL_USER = os.getenv("CLOUD_SQL_USER", "myuser")
CLOUD_SQL_PASSWORD = os.getenv("CLOUD_SQL_PASSWORD", "mypass")
CLOUD_SQL_DATABASE = os.getenv("CLOUD_SQL_DATABASE", "mydb")
CLOUD_SQL_TABLE_NAME = os.getenv("CLOUD_SQL_TABLE_NAME", "target_table")

# Batch Size
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))

# Optional: Use Workload Identity (default behavior)
USE_WORKLOAD_IDENTITY = os.getenv("USE_WORKLOAD_IDENTITY", "true").lower() == "true"

# ===========================
# 3. BIGQUERY CLIENT (WORKLOAD IDENTITY)
# ===========================
def get_bigquery_client() -> bigquery.Client:
    """Initialize BigQuery client using Google Auth Default (Workload Identity)."""
    try:
        credentials, project = default()
        client = bigquery.Client(credentials=credentials, project=project)
        logger.info(f"✅ Connected to BigQuery project: {project}")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to initialize BigQuery client: {e}")
        raise

# ===========================
# 4. DATABASE ENGINE (SQLALCHEMY + PYMYSQL)
# ===========================
def get_db_engine() -> Engine:
    """Create SQLAlchemy engine using PyMySQL."""
    try:
        db_url = f"mysql+pymysql://{CLOUD_SQL_USER}:{CLOUD_SQL_PASSWORD}@{CLOUD_SQL_HOST}:{CLOUD_SQL_PORT}/{CLOUD_SQL_DATABASE}"
        
        engine = create_engine(
            db_url,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            echo=False  # Set to True only for debugging
        )
        logger.info(f"✅ SQLAlchemy engine connected to MySQL: {CLOUD_SQL_DATABASE} @ {CLOUD_SQL_HOST}:{CLOUD_SQL_PORT}")
        return engine
    except Exception as e:
        logger.error(f"❌ Failed to connect to Cloud SQL: {e}")
        raise

# ===========================
# 5. DATA GENERATOR FROM BIGQUERY (STREAMING)
# ===========================
def fetch_bq_data_in_batches(client: bigquery.Client, dataset_id: str, table_id: str, batch_size: int) -> Generator[List[dict], None, None]:
    """
    Fetches data from BigQuery in batches using streaming.
    No schema check – assumes 1:1 match with target table.
    """
    full_table_id = f"{BQ_PROJECT_ID}.{dataset_id}.{table_id}"
    
    query = f"SELECT * FROM `{full_table_id}`"
    
    job_config = bigquery.QueryJobConfig(
        use_query_cache=True,
        use_legacy_sql=False,
        allow_large_results=True,
        destination=None,  # We're not writing to a table here
        page_size=batch_size,
    )

    try:
        query_job = client.query(query, job_config=job_config)
        
        # Stream results row by row
        iterator = query_job.result(page_size=batch_size)
        
        batch = []
        for row in iterator:
            # Convert Row object to dict
            row_dict = dict(row.items())
            batch.append(row_dict)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield last batch
        if batch:
            yield batch
            
        logger.info(f"✅ Data fetched from BQ: {full_table_id}")

    except Exception as e:
        logger.error(f"❌ Error fetching data from BigQuery: {e}")
        raise

# ===========================
# 6. INSERT INTO CLOUD SQL (SQLALCHEMY)
# ===========================
def insert_batch_to_mysql(engine: Engine, table_name: str, batch: List[dict]) -> int:
    """
    Insert a batch of rows into MySQL using SQLAlchemy.
    Uses bulk_insert_mappings for performance.
    """
    try:
        # Using raw SQL with execute many for better control
        # If you prefer ORM: session.bulk_insert_mappings(MyModel, batch)
        # But here we use direct execution for speed

        # Convert list of dicts to list of tuples for executemany
        columns = batch[0].keys()
        values = [tuple(row[col] for col in columns) for row in batch]

        # Construct dynamic insert statement
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join([f'`{c}`' for c in columns])}) VALUES ({placeholders})"
        
        with engine.begin() as conn:
            result = conn.execute(text(insert_sql), values)
            inserted_count = result.rowcount
            logger.info(f"📦 Inserted {inserted_count} rows into {table_name}")
            return inserted_count

    except Exception as e:
        logger.error(f"❌ Failed to insert batch into MySQL: {e}")
        raise

# ===========================
# 7. MAIN PIPELINE
# ===========================
def main():
    logger.info("🚀 Starting BigQuery → Cloud SQL Pipeline")

    # Step 1: Get clients
    bq_client = get_bigquery_client()
    db_engine = get_db_engine()

    # Step 2: Define source and target
    source_table = f"{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    target_table = CLOUD_SQL_TABLE_NAME

    total_rows = 0
    batch_count = 0

    try:
        # Step 3: Fetch and stream data from BQ
        for batch in fetch_bq_data_in_batches(bq_client, BQ_DATASET_ID, BQ_TABLE_ID, BATCH_SIZE):
            batch_count += 1
            inserted = insert_batch_to_mysql(db_engine, target_table, batch)
            total_rows += inserted

            # Log progress every 10 batches
            if batch_count % 10 == 0:
                logger.info(f"📊 Progress: {batch_count} batches processed, {total_rows} total rows loaded.")

        logger.info(f"🎉 Pipeline completed successfully!")
        logger.info(f"📊 Total Rows Loaded: {total_rows}")
        logger.info(f"📦 Total Batches Processed: {batch_count}")

    except Exception as e:
        logger.critical(f"🚨 Pipeline failed: {e}")
        raise

    finally:
        db_engine.dispose()
        logger.info("🧹 Database connection closed.")

# ===========================
# 8. OPTIONAL: RAW MYSQL CONNECTION (Pymysql Reference - COMMENTED OUT)
# ===========================
"""
# Example of how you could do it with raw pymysql (not recommended in this script)
def insert_with_pymysql(host, port, user, password, db, table, batch):
    import pymysql
    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=db,
        charset='utf8mb4'
    )
    try:
        cursor = conn.cursor()
        columns = ', '.join(batch[0].keys())
        placeholders = ', '.join(['%s'] * len(batch[0]))
        sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"
        values = [tuple(row[col] for col in batch[0].keys()) for row in batch]
        cursor.executemany(sql, values)
        conn.commit()
        logger.info(f"Inserted {cursor.rowcount} rows via pymysql")
    finally:
        conn.close()
"""

if __name__ == "__main__":
    main()
