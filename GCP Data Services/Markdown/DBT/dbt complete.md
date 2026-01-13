# DBT (Data Build Tool) in Data Engineering - Complete Guide

---

## 1. Introduction & Core Concepts

### What is DBT?
DBT transforms raw data into analytics-ready datasets using SQL. It enables version control, testing, documentation, and lineage tracking for data transformations.

**Key Capabilities:**
- Write transformations in SQL or Python
- Version control for data models
- Automated testing and validation
- Auto-generated documentation
- Data lineage tracking
- Incremental models for efficiency
- Modular, reusable transformations

---

## 2. DBT Architecture & Components

```
┌─────────────────────────────────────────────────────────┐
│                    DBT Project Structure                │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  my_dbt_project/                                        │
│  ├── dbt_project.yml          (Project config)          │
│  ├── profiles.yml             (Connection setup)        │
│  ├── models/                  (Transformation models)   │
│  │   ├── staging/             (Raw data cleaning)       │
│  │   │   ├── stg_customers.sql                         │
│  │   │   ├── stg_orders.sql                            │
│  │   │   └── _stg_sources.yml (Staging sources)        │
│  │   ├── intermediate/        (Business logic)          │
│  │   │   └── int_customer_orders.sql                   │
│  │   └── marts/               (Analytics/BI ready)      │
│  │       ├── fact_orders.sql                           │
│  │       └── dim_customers.sql                         │
│  ├── tests/                   (Data quality tests)      │
│  │   ├── generic/                                      │
│  │   └── singular/                                     │
│  ├── macros/                  (Reusable functions)      │
│  │   └── generate_schema_name.sql                      │
│  ├── data/                    (Seed files - static data)│
│  │   └── country_codes.csv                             │
│  ├── seeds/                   (Seed YAML configs)       │
│  └── target/                  (Compiled models)         │
│      └── compiled/                                     │
│          └── manifest.json    (Lineage & metadata)     │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## 3. DBT Initialization & Configuration

### dbt_project.yml - Main Configuration
```yaml
# dbt_project.yml
name: 'analytics_project'
version: '1.0.0'
config-version: 2

# Project directories
model-paths: ["models"]
test-paths: ["tests"]
data-paths: ["data"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

# Default configurations for all models
models:
  analytics_project:
    # All staging models are views by default
    staging:
      +materialized: view
      +schema: staging
    
    # Intermediate models are ephemeral (not materialized)
    intermediate:
      +materialized: ephemeral
      +schema: intermediate
    
    # Mart models are materialized tables
    marts:
      +materialized: table
      +schema: marts
      # Add indexes for performance
      +indexes:
        - columns: ['customer_id']
          type: 'hash'

# Seeds configuration
seeds:
  analytics_project:
    +schema: seeds
    +quote_columns: true

# Docs config
docs-paths: ["docs"]

# Profile connection
profile: 'analytics_project'

# Vars (runtime variables)
vars:
  'key_1': "{{ env_var('DBT_KEY_1') }}"
  'start_date': '2024-01-01'
  'lookback_days': 90
```

**Explanation:**
- `materialized`: View (virtual), Table (physical), Incremental, Ephemeral
- `schema`: Target database schema per model layer
- `vars`: Runtime variables for parameterized runs

### profiles.yml - Connection Configuration
```yaml
# ~/.dbt/profiles.yml
analytics_project:
  target: dev
  outputs:
    # Development environment
    dev:
      type: bigquery
      project: 'analytics-project-dev'
      dataset: 'dbt_dev'
      threads: 4
      timeout_seconds: 300
      location: 'US'
      priority: interactive
      retries: 1
      
    # Production environment
    prod:
      type: bigquery
      project: 'analytics-project-prod'
      dataset: 'dbt_prod'
      threads: 8
      timeout_seconds: 600
      location: 'US'
      priority: batch
      retries: 3
      
# For Snowflake
analytics_snowflake:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: xy12345
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: transformer
      database: analytics_db
      schema: dbt_prod
      threads: 4
      client_session_keep_alive: false
```

**Explanation:**
- Multiple environments (dev/prod) in one config
- Thread count controls parallelization
- Credentials via environment variables (secure)

---

## 4. Staging Layer - Data Cleaning

### stg_customers.sql - Cleaning Raw Data
```sql
-- models/staging/stg_customers.sql
{{ config(
    materialized='view',
    schema='staging',
    tags=['daily', 'customers'],
    description='Cleaned customer data from raw source'
) }}

WITH source_data AS (
  SELECT
    customer_id,
    customer_name,
    email,
    phone,
    country,
    created_at,
    updated_at
  FROM {{ source('raw', 'customers') }}  -- Reference to source
  WHERE customer_id IS NOT NULL
)

SELECT
  customer_id,
  UPPER(TRIM(customer_name)) as customer_name,
  LOWER(TRIM(email)) as email,
  REGEXP_REPLACE(phone, '[^0-9]', '') as phone_cleaned,
  CASE 
    WHEN country IS NULL THEN 'Unknown'
    ELSE UPPER(TRIM(country)) 
  END as country,
  CAST(created_at AS TIMESTAMP) as created_at,
  CAST(updated_at AS TIMESTAMP) as updated_at,
  CURRENT_TIMESTAMP() as _dbt_processed_at
FROM source_data
```

**Explanation:**
- `{{ source() }}`: References raw source tables (tracked in YAML)
- `{{ config() }}`: Model-specific configuration
- Cleans: nulls, case inconsistency, formats
- `_dbt_processed_at`: Added by dbt for tracking

### stg_orders.sql - Multi-source Join
```sql
-- models/staging/stg_orders.sql
{{ config(
    materialized='view',
    description='Cleaned order data'
) }}

WITH orders AS (
  SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    amount,
    status
  FROM {{ source('raw', 'orders') }}
  WHERE order_date >= '2024-01-01'
)

SELECT
  order_id,
  customer_id,
  product_id,
  CAST(order_date AS DATE) as order_date,
  ROUND(CAST(amount AS FLOAT64), 2) as amount,
  UPPER(TRIM(status)) as status,
  CURRENT_TIMESTAMP() as _dbt_processed_at
FROM orders
WHERE amount > 0  -- Remove invalid records
```

### sources.yml - Source Definitions
```yaml
# models/staging/_stg_sources.yml
version: 2

sources:
  - name: raw  # Source dataset name
    description: Raw data from transactional systems
    database: analytics-project-prod
    schema: raw_data
    tables:
      - name: customers
        description: Customer master data from CRM
        columns:
          - name: customer_id
            description: Unique customer identifier
            tests:
              - unique
              - not_null
          - name: email
            description: Customer email address
            tests:
              - unique
      
      - name: orders
        description: Order transactions
        columns:
          - name: order_id
            tests:
              - unique
              - not_null
          - name: customer_id
            tests:
              - relationships  # Foreign key check
                to: source('raw', 'customers')
                field: customer_id
```

**Explanation:**
- `source()`: Single source of truth for data origins
- `tests`: Built-in data quality validation
- `relationships`: Foreign key constraints

---

## 5. Intermediate Layer - Business Logic

### int_customer_orders.sql - Aggregation
```sql
-- models/intermediate/int_customer_orders.sql
{{ config(
    materialized='ephemeral',  -- Not stored, only referenced
    description='Customer order summary for aggregation'
) }}

SELECT
  c.customer_id,
  c.customer_name,
  c.country,
  COUNT(DISTINCT o.order_id) as total_orders,
  SUM(o.amount) as lifetime_value,
  AVG(o.amount) as avg_order_value,
  MIN(o.order_date) as first_order_date,
  MAX(o.order_date) as last_order_date,
  DATE_DIFF(CURRENT_DATE(), MAX(o.order_date), DAY) as days_since_last_order
FROM {{ ref('stg_customers') }} c  -- ref() = model dependency
LEFT JOIN {{ ref('stg_orders') }} o
  ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.country
```

**Explanation:**
- `{{ ref() }}`: Creates dependency between models
- `ephemeral`: Inlined into dependent queries (no table created)
- Aggregates customer metrics for reuse

### int_customer_segmentation.sql - Segmentation Logic
```sql
-- models/intermediate/int_customer_segmentation.sql
{{ config(materialized='ephemeral') }}

WITH customer_metrics AS (
  SELECT
    *,
    DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) as recency_days
  FROM {{ ref('int_customer_orders') }}
)

SELECT
  customer_id,
  customer_name,
  country,
  total_orders,
  lifetime_value,
  recency_days,
  CASE
    WHEN recency_days <= 30 AND lifetime_value > 1000 THEN 'VIP'
    WHEN recency_days <= 90 AND total_orders > 5 THEN 'Loyal'
    WHEN recency_days > 180 THEN 'Inactive'
    ELSE 'Active'
  END as customer_segment,
  NTILE(5) OVER (ORDER BY lifetime_value DESC) as revenue_quintile
FROM customer_metrics
```

---

## 6. Mart Layer - Analytics Ready Models

### fact_orders.sql - Fact Table
```sql
-- models/marts/fact_orders.sql
{{ config(
    materialized='incremental',
    unique_id='order_id',
    on_schema_change='fail',
    indexes=[
      {'columns': ['customer_id'], 'type': 'hash'},
      {'columns': ['order_date'], 'type': 'btree'}
    ]
) }}

SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  o.order_date,
  o.amount,
  o.status,
  CAST(DATE_TRUNC(o.order_date, MONTH) AS DATE) as order_month,
  EXTRACT(YEAR FROM o.order_date) as order_year,
  cs.customer_segment,
  cs.revenue_quintile,
  CURRENT_TIMESTAMP() as _dbt_processed_at
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('int_customer_segmentation') }} cs
  ON o.customer_id = cs.customer_id

{% if execute %}
  -- Only load new data in incremental runs
  WHERE o.order_date >= (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

**Explanation:**
- `incremental`: Load only new records (efficient)
- `unique_id`: Defines primary key
- `on_schema_change`: Fail on schema mismatch (safety)
- `{% if execute %}`: Conditional SQL for incremental

### dim_customers.sql - Dimension Table
```sql
-- models/marts/dim_customers.sql
{{ config(
    materialized='table',
    unique_key='customer_id',
    description='Customer dimension with SCD Type 1'
) }}

SELECT
  c.customer_id,
  c.customer_name,
  c.email,
  c.phone_cleaned as phone,
  c.country,
  cs.customer_segment,
  cs.revenue_quintile,
  cs.total_orders,
  cs.lifetime_value,
  c.created_at,
  c.updated_at,
  CURRENT_TIMESTAMP() as _dbt_updated_at
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('int_customer_segmentation') }} cs
  ON c.customer_id = cs.customer_id
```

---

## 7. Testing Framework

### Schema.yml - Column Tests
```yaml
# models/marts/schema.yml
version: 2

models:
  - name: fact_orders
    description: Core fact table for order analytics
    columns:
      - name: order_id
        description: Unique order identifier
        tests:
          - unique      # No duplicates
          - not_null    # No nulls
      
      - name: customer_id
        description: Foreign key to dim_customers
        tests:
          - not_null
          - relationships:  # Foreign key constraint
              to: ref('dim_customers')
              field: customer_id
      
      - name: amount
        description: Order amount in USD
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_positive  # Amount > 0
      
      - name: order_date
        description: Date order was placed
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_in_type_list:
              column_type_list: ['timestamp', 'date']
```

### Singular Tests - Custom SQL Tests
```sql
-- tests/assert_orders_have_amounts.sql
-- Custom test: ensures all orders have positive amounts

SELECT *
FROM {{ ref('fact_orders') }}
WHERE amount <= 0
  OR amount IS NULL
  OR status NOT IN ('completed', 'pending', 'cancelled')
```

**Explanation:**
- Returns rows if test fails (no rows = pass)
- Custom validation logic

### Generic Tests - Reusable Tests
```sql
-- macros/tests/test_revenue_not_negative.sql
{% test revenue_not_negative(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} < 0
{% endtest %}

-- Usage in schema.yml:
# tests:
#   - revenue_not_negative:
#       column_name: lifetime_value
```

---

## 8. Data Migration Use Cases

### Use Case 1: Migrating from Legacy System to BigQuery

```sql
-- models/staging/stg_legacy_customers.sql
-- Migration: Extract from old database, clean, load to warehouse

{{ config(
    materialized='view',
    schema='staging',
    tags=['migration', 'legacy_system']
) }}

WITH legacy_data AS (
  SELECT
    cust_id as customer_id,
    cust_name as customer_name,
    cust_email as email,
    cust_country as country,
    cust_created as created_at
  FROM {{ source('legacy_system', 'customer_master') }}
  WHERE cust_status = 'ACTIVE'
)

SELECT
  customer_id,
  customer_name,
  email,
  country,
  created_at,
  'MIGRATED' as migration_status,
  CURRENT_TIMESTAMP() as migration_date
FROM legacy_data
```

### Use Case 2: Data Reconciliation Post-Migration

```sql
-- tests/test_migration_reconciliation.sql
-- Verify record counts match source and target

SELECT
  'Source' as system,
  COUNT(*) as record_count
FROM {{ source('legacy_system', 'customer_master') }}
WHERE cust_status = 'ACTIVE'

UNION ALL

SELECT
  'Target' as system,
  COUNT(*) as record_count
FROM {{ ref('stg_legacy_customers') }}
HAVING COUNT(*) != (
  SELECT COUNT(*)
  FROM {{ source('legacy_system', 'customer_master') }}
  WHERE cust_status = 'ACTIVE'
)
```

### Use Case 3: Incremental Migration with Watermarking

```sql
-- models/staging/stg_migrated_orders.sql
-- Migrate orders in batches using watermark

{{ config(
    materialized='incremental',
    unique_id='order_id',
    tags=['migration']
) }}

SELECT
  order_id,
  customer_id,
  order_amount,
  order_date,
  CURRENT_TIMESTAMP() as migration_timestamp
FROM {{ source('legacy_system', 'orders') }}

{% if execute %}
  WHERE order_date >= 
    COALESCE(
      (SELECT MAX(order_date) FROM {{ this }}),
      CAST('2020-01-01' AS DATE)
    )
{% endif %}
```

**Explanation:**
- Loads data in chunks since migration start
- Avoids re-processing old records
- Safe for large migrations

### Use Case 4: Dual System Validation During Migration

```sql
-- tests/test_legacy_vs_new_system.sql
-- Compare data between old and new systems during transition

WITH legacy_summary AS (
  SELECT
    customer_id,
    COUNT(*) as legacy_order_count,
    SUM(amount) as legacy_total
  FROM {{ source('legacy_system', 'orders') }}
  GROUP BY customer_id
),

new_system_summary AS (
  SELECT
    customer_id,
    COUNT(*) as new_order_count,
    SUM(amount) as new_total
  FROM {{ ref('fact_orders') }}
  GROUP BY customer_id
)

SELECT
  l.customer_id,
  l.legacy_order_count,
  n.new_order_count,
  ROUND(ABS(l.legacy_total - n.new_total), 2) as amount_variance
FROM legacy_summary l
FULL OUTER JOIN new_system_summary n
  ON l.customer_id = n.customer_id
WHERE l.legacy_order_count != n.new_order_count
  OR ROUND(ABS(l.legacy_total - n.new_total), 2) > 0.01
```

---

## 9. Macros - Reusable Functions

### Custom Macro for Date Bucketing
```sql
-- macros/date_spine.sql
-- Generate all dates in a range

{% macro generate_date_spine(begin_date, end_date) %}

WITH date_spine AS (
  SELECT
    CAST(d AS DATE) as date_day
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      CAST('{{ begin_date }}' AS DATE),
      CAST('{{ end_date }}' AS DATE),
      INTERVAL 1 DAY
    )
  ) as d
)

SELECT * FROM date_spine

{% endmacro %}

-- Usage in model:
-- FROM {{ generate_date_spine('2024-01-01', '2024-12-31') }}
```

### Macro for Dynamic Schema Names
```sql
-- macros/generate_schema_name.sql
-- Prefix schemas based on environment

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
```

### Macro for Data Quality Checks
```sql
-- macros/data_quality_checks.sql
-- Reusable data quality validation

{% macro run_quality_checks(table_name, column_name, expected_min, expected_max) %}

  {%- set query -%}
    SELECT
      COUNT(*) as total_rows,
      COUNTIF({{ column_name }} IS NULL) as null_count,
      MIN({{ column_name }}) as min_value,
      MAX({{ column_name }}) as max_value,
      COUNT(DISTINCT {{ column_name }}) as distinct_count
    FROM {{ table_name }}
  {%- endset -%}

  {%- set results = run_query(query) -%}

  {%- if execute -%}
    {%- for row in results -%}
      {%- if row.min_value < expected_min or row.max_value > expected_max -%}
        {{ exceptions.raise_compiler_error("Quality check failed for " ~ table_name) }}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

{% endmacro %}
```

---

## 10. Variables & Parameterization

### Using Variables for Dynamic Filters
```yaml
# dbt_project.yml
vars:
  lookback_days: 90
  min_order_amount: 10.00
  countries: ['USA', 'Canada', 'UK']
```

```sql
-- models/marts/fact_recent_orders.sql
{{ config(materialized='table') }}

SELECT *
FROM {{ ref('fact_orders') }}
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
  AND amount >= {{ var('min_order_amount') }}
  AND country IN ('{{ var("countries") | join("', '") }}')
```

**Explanation:**
- Override vars at runtime: `dbt run --vars '{"lookback_days": 180}'`
- Enables parameterized pipelines

---

## 11. Seeds - Static Reference Data

### countries.csv - Static Data
```csv
country_code,country_name,region,timezone
US,United States,North America,UTC-5
CA,Canada,North America,UTC-5
GB,United Kingdom,Europe,UTC+0
AU,Australia,Asia-Pacific,UTC+10
```

### Loading Seeds into Database
```bash
dbt seed
# Creates table: analytics.seeds.countries
```

```sql
-- Usage in model:
SELECT
  o.*,
  c.region,
  c.timezone
FROM {{ ref('fact_orders') }} o
LEFT JOIN {{ ref('countries') }} c
  ON o.country = c.country_code
```

---

## 12. Snapshots - Type 2 SCD (Slowly Changing Dimensions)

### Snapshot Configuration
```sql
-- snapshots/customers_snapshot.sql
-- Tracks historical changes to customer dimension

{% snapshot customers_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
  }}

  SELECT
    customer_id,
    customer_name,
    email,
    country,
    created_at,
    updated_at
  FROM {{ source('raw', 'customers') }}

{% endsnapshot %}

-- Run: dbt snapshot
-- Creates table with _scd_id, dbt_valid_from, dbt_valid_to
```

**Explanation:**
- Tracks all dimension changes over time
- `dbt_valid_from`: When record became current
- `dbt_valid_to`: When record was superseded
- Enables historical analysis

---

## 13. Documentation

### Auto-Generated Docs
```yaml
# models/marts/schema.yml
version: 2

models:
  - name: fact_orders
    description: |
      Core fact table containing order transactions.
      Updated daily with new orders.
      
      **Owner:** Analytics Team
      **Freshness:** Daily
      **SLA:** 2 hours after source update
    
    columns:
      - name: order_id
        description: Unique order identifier
        tests: [unique, not_null]
      
      - name: customer_id
        description: |
          Foreign key to dim_customers.
          Represents the customer who placed the order.
```

```bash
# Generate documentation
dbt docs generate

# Serve locally
dbt docs serve
# Opens: http://localhost:8000
```

---

## 14. Data Lineage & Visualization

### Understanding Lineage
```
Raw Data Sources
    ↓
    ├─→ stg_customers.sql (Staging)
    │   ↓
    │   ├─→ int_customer_segmentation.sql (Intermediate)
    │   │   ↓
    │   └─→ dim_customers.sql (Mart)
    │
    ├─→ stg_orders.sql (Staging)
    │   ↓
    │   ├─→ int_customer_orders.sql (Intermediate)
    │   │   ↓
    │   ├─→ fact_orders.sql (Mart)
    │   └─→ int_customer_segmentation.sql (Intermediate)
    │
    └─→ Other Staging Models
        ↓
        └─→ Intermediate Models
            ↓
            └─→ Mart Models (Analytics Ready)

# View lineage:
dbt docs generate
# Opens DAG visualization in web UI
```

---

## 15. DBT CLI Commands

### Common Commands
```bash
# Initialize new DBT project
dbt init analytics_project

# Validate syntax and dependencies
dbt parse

# Run all models
dbt run

# Run specific model and dependents
dbt run --select fact_orders+

# Run staging layer only
dbt run --select path:models/staging

# Run with specific threads
dbt run --threads 8

# Run tests
dbt test

# Run specific test
dbt test --select stg_customers

# Generate documentation
dbt docs generate
dbt docs serve

# Create snapshots (SCD Type 2)
dbt snapshot

# Seed static data
dbt seed

# Fresh run (rebuild everything)
dbt run --full-refresh

# Dry run (compile without executing)
dbt compile

# Debug connection
dbt debug

# Run with variables
dbt run --vars '{"lookback_days": 180}'

# Run on schedule (via scheduler)
0 2 * * * cd /path/to/project && dbt run

# Cloud: Deploy to production
dbt run --target prod
```

---

## 16. DBT Cloud Integration

### Cloud Job Configuration (YAML)
```yaml
# dbt_cloud_job.yml
name: "Daily Production Run"
description: "Run production transformations nightly"

triggers:
  github_webhook: true  # Trigger on PR/commit
  schedule:
    interval: 0
    hour: 2  # 2 AM daily
    day_of_week: null
    threads: 8

commands:
  - dbt deps          # Install dependencies
  - dbt snapshot      # Create SCD snapshots
  - dbt run --full-refresh --target prod  # Full run in prod
  - dbt test          # Run all tests
  - dbt docs generate # Generate docs

notifications:
  - type: email
    state: fail
    include_log: true
  - type: slack
    state: all
    webhook_url: https://hooks.slack.com/...
```

---

## 17. Performance Optimization

### Incremental Models
```sql
-- models/marts/fact_events.sql
-- Load only new events since last run

{{ config(
    materialized='incremental',
    unique_id='event_id',
    on_schema_change='sync_all_columns',
    incremental_strategy='merge'
) }}

SELECT
  event_id,
  user_id,
  event_type,
  event_timestamp,
  properties
FROM {{ source('analytics', 'raw_events') }}

{% if execute %}
  WHERE event_timestamp > (SELECT COALESCE(MAX(event_timestamp), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
{% endif %}
```

### Materialized Views for Complex Aggregations
```sql
-- models/marts/customer_summary_mv.sql
-- Materialized view for frequently accessed aggregations

{{ config(
    materialized='materialized_view',
    schema='marts'
) }}

SELECT
  customer_id,
  COUNT(DISTINCT order_id) as total_orders,
  SUM(amount) as lifetime_value
FROM {{ ref('fact_orders') }}
GROUP BY customer_id
```

---

## 18. Python Models (dbt-python)

### Python Transformation Model
```python
# models/marts/customer_clusters.py
# ML clustering using Python instead of SQL

import pandas as pd
from sklearn.cluster import KMeans

def model(dbt, session):
    """
    Runs customer clustering using K-means
    dbt: dbt object for accessing relations
    session: Snowpark/Spark session
    """
    
    # Get input data
    customer_features = dbt.ref('customer_features')
    df = customer_features.to_pandas()
    
    # Prepare features for clustering
    X = df[['lifetime_value', 'total_orders', 'days_since_purchase']]
    
    # Run K-means clustering
    kmeans = KMeans(n_clusters=5, random_state=42)
    df['cluster'] = kmeans.fit_predict(X)
    
    # Return results
    return df

# config in yml:
# materialized: 'python'
# packages:
#   - scikit-learn
#   - pandas
```

---

## 19. DBT Workflow Example (Complete Pipeline)

### Step 1: Initialize & Configure
```bash
dbt init my_analytics
cd my_analytics
# Edit dbt_project.yml and profiles.yml
```

### Step 2: Create Staging Models
```bash
# models/staging/stg_customers.sql
# models/staging/stg_orders.sql
# models/staging/_stg_sources.yml
```

### Step 3: Create Intermediate Models
```bash
# models/intermediate/int_customer_orders.sql
# models/intermediate/int_customer_segmentation.sql
```

### Step 4: Create Mart Models
```bash
# models/marts/fact_orders.sql
# models/marts/dim_customers.sql
# models/marts/schema.yml (with tests)
```

### Step 5: Run & Validate
```bash
dbt parse                    # Check syntax
dbt run --target dev         # Build dev models
dbt test                     # Run quality tests
dbt snapshot                 # Create SCD snapshots
dbt docs generate            # Generate docs
dbt docs serve               # View documentation
```

### Step 6: Deploy to Production
```bash
dbt run --target prod --full-refresh
dbt test --target prod
# Push to GitHub for version control
git add .
git commit -m "Production deployment"
git push origin main
```

---

## 20. Complete Data Migration Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                   DATA MIGRATION LIFECYCLE WITH DBT                  │
└──────────────────────────────────────────────────────────────────────┘

PHASE 1: ASSESSMENT & PLANNING
┌─────────────────────────────────────────────────────────────────┐
│                  Source System Analysis                         │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐   │
│  │ Legacy DB   │→ │ Data Profiling│→ │ Create DBT Models   │   │
│  │ (Oracle,    │  │ (Row count,   │  │ (Staging Layer)     │   │
│  │ MySQL,MSSQL)│  │  nulls, types)│  │                     │   │
│  └─────────────┘  └──────────────┘  └─────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 2: INITIAL EXTRACTION
┌─────────────────────────────────────────────────────────────────┐
│            Extract Raw Data to Intermediate Layer               │
│  ┌──────────────────┐  ┌─────────────────────┐                 │
│  │ DBT Source Refs  │→ │ GCS/Cloud Storage   │                 │
│  │ (staging models) │  │ (Raw CSV/Parquet)   │                 │
│  └──────────────────┘  └─────────────────────┘                 │
│         ↓                        ↓                              │
│  ┌──────────────────┐  ┌─────────────────────┐                 │
│  │ Load to BigQuery │→ │ Staging Tables      │                 │
│  │ (WRITE_TRUNCATE) │  │ (stg_* models)      │                 │
│  └──────────────────┘  └─────────────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 3: DATA CLEANING & VALIDATION
┌─────────────────────────────────────────────────────────────────┐
│                    Data Quality Checks                          │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │ Run dbt test     │  │ Check for:       │                    │
│  │ on staging       │  │ • Duplicates     │                    │
│  │ (assert nulls,   │  │ • Missing values │                    │
│  │  duplicates,     │→ │ • Invalid dates  │                    │
│  │  referential     │  │ • Format issues  │                    │
│  │  integrity)      │  │ • Type mismatches│                    │
│  └──────────────────┘  └──────────────────┘                    │
│         ↓                        ↓                              │
│  ┌──────────────────────────────────────────┐                  │
│  │ If Tests Pass: Proceed to Cleaning      │                  │
│  │ If Tests Fail: Log & Quarantine Data     │                  │
│  └──────────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 4: DATA TRANSFORMATION
┌─────────────────────────────────────────────────────────────────┐
│               Clean & Standardize Data                          │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │ Trim/Uppercase   │  │ Type Conversion  │                    │
│  │ Remove Nulls     │→ │ Date Formatting  │                    │
│  │ Deduplication    │  │ String Cleaning  │                    │
│  │ Format Fix       │  │ Regex Validation │                    │
│  └──────────────────┘  └──────────────────┘                    │
│         ↓                                                       │
│  ┌──────────────────────────────────────────┐                  │
│  │ Create Intermediate Models               │                  │
│  │ (int_* with business logic)              │                  │
│  └──────────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 5: ENRICHMENT & REFERENCE DATA
┌─────────────────────────────────────────────────────────────────┐
│           Join with Master & Reference Tables                   │
│  ┌────────────────────┐  ┌────────────────────┐                │
│  │ Geography Lookup   │  │ Product Master     │                │
│  │ (regions, timezones)│ │ (categories, attrs)│                │
│  └────────────────────┘  └────────────────────┘                │
│         ↓                        ↓                              │
│  ┌────────────────────────────────────────┐                    │
│  │ Create Enriched Intermediate Models    │                    │
│  └────────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 6: MART CREATION (ANALYTICS READY)
┌─────────────────────────────────────────────────────────────────┐
│            Build Analytics Tables (Fact & Dimension)            │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │ Fact Tables      │  │ Dimension Tables │                    │
│  │ (fact_*)         │  │ (dim_*)          │                    │
│  │ • fact_orders    │→ │ • dim_customers  │                    │
│  │ • fact_events    │  │ • dim_products   │                    │
│  │ • fact_payments  │  │ • dim_dates      │                    │
│  └──────────────────┘  └──────────────────┘                    │
│         ↓                        ↓                              │
│  ┌──────────────────────────────────────┐                      │
│  │ Incremental Load (merge strategy)    │                      │
│  │ Only append new data since last run  │                      │
│  └──────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 7: DUAL SYSTEM VALIDATION
┌─────────────────────────────────────────────────────────────────┐
│         Compare Legacy System vs New Warehouse                  │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │ Legacy System    │  │ BigQuery Tables  │                    │
│  │ SELECT COUNT(*) │→ │ SELECT COUNT(*)  │                    │
│  │ SELECT SUM(amt) │  │ SELECT SUM(amt)  │                    │
│  └──────────────────┘  └──────────────────┘                    │
│         ↓                        ↓                              │
│  ┌──────────────────────────────────────┐                      │
│  │ Reconciliation Report                │                      │
│  │ - Row count match: ✓                 │                      │
│  │ - Amount totals: ✓ (variance < 0.01%)│                      │
│  │ - Key lookups: ✓                     │                      │
│  └──────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 8: SNAPSHOTS & SCD
┌─────────────────────────────────────────────────────────────────┐
│       Track Historical Changes (Type 2 SCD)                     │
│  ┌──────────────────────────────────────┐                      │
│  │ dbt snapshot on dim_customers        │                      │
│  │ Tracks:                              │                      │
│  │ • dbt_valid_from (when row active)   │                      │
│  │ • dbt_valid_to (when superseded)     │                      │
│  │ • dbt_scd_id (surrogate key)         │                      │
│  └──────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 9: CUTOVER & GO-LIVE
┌─────────────────────────────────────────────────────────────────┐
│              Switch from Legacy to New System                   │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │ Legacy System    │  │ New Warehouse    │                    │
│  │ (Read-only)      │→ │ (Primary Source) │                    │
│  │ (Archival)       │  │ (BigQuery)       │                    │
│  └──────────────────┘  └──────────────────┘                    │
│         ↓                        ↓                              │
│  ┌──────────────────────────────────────┐                      │
│  │ Switch BI Tools & Reports            │                      │
│  │ Point from old warehouse to new      │                      │
│  └──────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
                            ↓

PHASE 10: POST-MIGRATION MONITORING
┌─────────────────────────────────────────────────────────────────┐
│           Monitor & Maintain New Warehouse                      │
│  ┌──────────────────┐  ┌──────────────────┐                    │
│  │ Daily dbt run    │  │ Monitor Tests    │                    │
│  │ (on schedule)    │→ │ • Freshness      │                    │
│  │ • Run transform. │  │ • Accuracy       │                    │
│  │ • Run tests      │  │ • Uniqueness     │                    │
│  │ • Generate docs  │  │ • Relationships  │                    │
│  └──────────────────┘  └──────────────────┘                    │
│         ↓                        ↓                              │
│  ┌──────────────────────────────────────┐                      │
│  │ Set SLAs & Alerts                    │                      │
│  │ • Data freshness (2 hours)           │                      │
│  │ • Test failure alerts (Slack/Email)  │                      │
│  │ • Performance monitoring              │                      │
│  └──────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘

```

---

## 21. Migration Strategy Comparison

| Strategy | Approach | Use Case | Risk |
|----------|----------|----------|------|
| **Big Bang** | Migrate all data at once | Small datasets (<1GB) | High - no fallback |
| **Phased** | Migrate by table/module | Medium datasets (1-100GB) | Medium - parallel run period |
| **Incremental** | Daily watermark sync | Large datasets (>100GB) | Low - gradual cutover |
| **Dual Run** | Run both systems in parallel | Critical systems | Medium - need reconciliation |

---

## 22. Migration Reconciliation Queries

### Table-Level Reconciliation
```sql
-- tests/reconciliation/customer_count_reconciliation.sql
-- Ensure record counts match between systems

WITH legacy_count AS (
  SELECT COUNT(*) as legacy_records
  FROM {{ source('legacy_system', 'customers') }}
  WHERE status = 'ACTIVE'
),

new_count AS (
  SELECT COUNT(*) as new_records
  FROM {{ ref('dim_customers') }}
  WHERE is_active = TRUE
)

SELECT
  l.legacy_records,
  n.new_records,
  ABS(l.legacy_records - n.new_records) as discrepancy
FROM legacy_count l
CROSS JOIN new_count n
WHERE l.legacy_records != n.new_records
```

### Row-Level Reconciliation
```sql
-- tests/reconciliation/customer_detail_reconciliation.sql
-- Find specific record mismatches

WITH legacy_data AS (
  SELECT
    cust_id,
    UPPER(cust_name) as name,
    LOWER(cust_email) as email,
    CAST(total_spent AS DECIMAL(10,2)) as spent
  FROM {{ source('legacy_system', 'customer_summary') }}
),

new_data AS (
  SELECT
    customer_id,
    customer_name as name,
    email,
    lifetime_value as spent
  FROM {{ ref('dim_customers') }}
),

comparison AS (
  SELECT
    COALESCE(l.cust_id, n.customer_id) as customer_id,
    CASE
      WHEN l.cust_id IS NULL THEN 'Only in New'
      WHEN n.customer_id IS NULL THEN 'Only in Legacy'
      WHEN l.name != n.name THEN 'Name Mismatch'
      WHEN l.email != n.email THEN 'Email Mismatch'
      WHEN l.spent != n.spent THEN 'Amount Mismatch'
      ELSE 'Match'
    END as reconciliation_status
  FROM legacy_data l
  FULL OUTER JOIN new_data n
    ON l.cust_id = n.customer_id
)

SELECT *
FROM comparison
WHERE reconciliation_status != 'Match'
```

---

## 23. Performance Tuning in DBT

### Query Optimization Pattern
```sql
-- models/marts/fact_orders_optimized.sql
-- Performance tuning with clustering and partitioning

{{ config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['customer_id', 'product_id'],
    kms_key_name='projects/my-project/locations/us/keyRings/my-keyring/cryptoKeys/my-key',
    tags=['high_volume', 'critical']
) }}

SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  DATE(o.order_date) as order_date,
  o.amount,
  c.customer_segment,
  p.product_category
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c
  ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_products') }} p
  ON o.product_id = p.product_id
WHERE o.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
```

### Index Creation for Performance
```sql
-- macros/create_indexes.sql
-- Create indexes on frequently queried columns

{% macro create_indexes(table_name, columns) %}
  {% if execute %}
    {% set index_ddl %}
      {% for column in columns %}
        CREATE INDEX idx_{{ table_name }}_{{ column }} 
        ON `{{ table_name }}` ({{ column }})
        {% if not loop.last %};{% endif %}
      {% endfor %}
    {% endset %}
    
    {% do run_query(index_ddl) %}
  {% endif %}
{% endmacro %}

-- Usage:
-- {{ create_indexes('my_table', ['customer_id', 'date_field']) }}
```

---

## 24. Advanced DBT Patterns for Migrations

### Feature Flags During Migration
```sql
-- models/marts/orders_with_feature_flags.sql
-- Gradually switch between old and new data

{{ config(materialized='table') }}

{% set use_new_warehouse = var('use_new_warehouse', false) %}

{% if use_new_warehouse %}
  -- Use new data warehouse
  SELECT
    order_id,
    customer_id,
    amount,
    order_date,
    'NEW_WAREHOUSE' as data_source
  FROM {{ ref('fact_orders') }}
{% else %}
  -- Use legacy system (fallback)
  SELECT
    order_id,
    customer_id,
    amount,
    order_date,
    'LEGACY_SYSTEM' as data_source
  FROM {{ source('legacy_system', 'orders') }}
{% endif %}

-- Run with: dbt run --vars '{"use_new_warehouse": true}'
```

### A/B Testing Data Quality
```sql
-- tests/test_data_comparison_ab.sql
-- Compare metrics between legacy and new system

SELECT
  'Legacy' as system,
  COUNT(*) as record_count,
  COUNT(DISTINCT customer_id) as unique_customers,
  SUM(amount) as total_amount
FROM {{ source('legacy_system', 'orders') }}

UNION ALL

SELECT
  'New Warehouse' as system,
  COUNT(*) as record_count,
  COUNT(DISTINCT customer_id) as unique_customers,
  SUM(amount) as total_amount
FROM {{ ref('fact_orders') }}
```

---

## 25. DBT + Data Orchestration (Airflow Integration)

### Running DBT from Airflow
```python
# dags/dbt_migration_pipeline.py
# Orchestrate DBT with Airflow

from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_migration_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
)

# Pre-migration checks
pre_checks = BashOperator(
    task_id='pre_migration_checks',
    bash_command='dbt parse && dbt compile',
    dag=dag
)

# Run DBT via Cosmos (dbt-airflow integration)
dbt_run = DbtTaskGroup(
    group_id='dbt_migration',
    project_config=ProjectConfig(
        dbt_project_path='/path/to/dbt/project',
        manifest_path='/path/to/dbt/target/manifest.json'
    ),
    render_config=RenderConfig(
        select=['models/staging+', 'models/intermediate+', 'models/marts+'],
        exclude=['path:models/experimental/*']
    ),
    dag=dag
)

# Post-migration validation
post_validation = BashOperator(
    task_id='post_migration_validation',
    bash_command='dbt test --select tag:migration',
    dag=dag
)

# Reconciliation report
reconciliation = BashOperator(
    task_id='run_reconciliation',
    bash_command='dbt test --select path:tests/reconciliation',
    dag=dag
)

# Set dependencies
pre_checks >> dbt_run >> post_validation >> reconciliation
```

---

## 26. Troubleshooting Common DBT Issues

### Issue 1: Circular Dependencies
```
Error: Circular reference detected
Solution: Review model dependencies, break cycles by using ephemeral models

# Check with:
dbt test --select models/
dbt run --select models/ --dry-run
```

### Issue 2: Source Table Not Found
```sql
-- Solution: Verify source definition
-- models/staging/_stg_sources.yml
sources:
  - name: raw
    database: correct-project-id
    schema: correct_schema
    tables:
      - name: customers
        # Verify table exists in source system
```

### Issue 3: Test Failures on Incremental Models
```sql
-- Solution: Refresh incremental model
-- CLI: dbt run --full-refresh --select fact_orders
-- Or in model config:
{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns'  # Auto-sync schema changes
) }}
```

### Issue 4: Memory Issues on Large Datasets
```sql
-- Solution: Use incremental strategy for large tables
{{ config(
    materialized='incremental',
    unique_id='order_id',
    incremental_strategy='merge',  # More efficient than delete+insert
    post_hook='ALTER TABLE {{ this }} ADD COLUMN processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()'
) }}
```

---

## 27. Migration Timeline & Checklist

```
WEEK 1: Planning & Assessment
□ Profile source system (row counts, schemas, dependencies)
□ Create DBT project structure
□ Set up dev environment (BigQuery, GCS, DBT Cloud)
□ Document source tables and business rules

WEEK 2-3: Initial Development
□ Create staging models (stg_*)
□ Add source definitions and basic tests
□ Implement data cleaning logic
□ Run initial dbt test on staging

WEEK 4: Intermediate & Enrichment
□ Create intermediate models (int_*)
□ Add reference data joins
□ Implement business logic
□ Add complex tests

WEEK 5: Mart Layer & Analytics
□ Create fact tables (fact_*)
□ Create dimension tables (dim_*)
□ Set up incremental load strategy
□ Create snapshots for SCD Type 2

WEEK 6: Validation & Testing
□ Write reconciliation queries
□ Run dual system validation
□ Fix data discrepancies
□ Document findings

WEEK 7: Performance & Optimization
□ Analyze query performance
□ Add clustering & partitioning
□ Optimize incremental strategies
□ Load testing with full volume

WEEK 8: Migration & Cutover
□ Final data load from legacy system
□ Switch BI tools to new warehouse
□ Monitor data quality metrics
□ Archive legacy system (read-only)

WEEK 9+: Monitoring & Maintenance
□ Daily dbt runs & test validation
□ Monitor performance metrics
□ Handle edge cases & exceptions
□ Optimize based on usage patterns
```

---

## 28. Key Takeaways: DBT for Data Engineering

| Aspect | Benefit |
|--------|---------|
| **Version Control** | Track all transformations in Git |
| **Testing** | Automated data quality validation |
| **Documentation** | Auto-generated lineage & docs |
| **Modularity** | Reusable models & macros |
| **Incremental Loading** | Process only new data (cost & time) |
| **Collaboration** | Code reviews via Git PRs |
| **Monitoring** | Built-in freshness & quality checks |
| **Scalability** | Handles 100GB+ datasets |
| **Migration Ready** | Staged approach reduces risk |
| **Data Governance** | Clear lineage & dependencies |

---

## 29. DBT Ecosystem Tools

### Related Tools
- **dbt-core**: Open source transformation engine
- **dbt-cloud**: Managed hosting, scheduling, CI/CD
- **Soda**: Data quality monitoring
- **Great Expectations**: Advanced testing framework
- **dbt-utils**: Reusable macros library
- **dbt-expectations**: Statistical tests (outlier detection)
- **dbt-codegen**: Auto-generate models from schema

### Integration Examples
```bash
# Install dbt packages
dbt deps

# packages.yml example:
packages:
  - package: dbt-labs/dbt_utils
    version: 1.0.0
  - package: calogica/dbt_expectations
    version: 0.8.0
```

---

## Summary: DBT Data Migration Workflow

```
Legacy System  →  GCS/Cloud Storage  →  BigQuery Staging
     ↓                                        ↓
  [Extract]                            [Load & Validate]
                                             ↓
                                    [Clean & Standardize]
                                      (Staging Models)
                                             ↓
                                   [Business Logic]
                                 (Intermediate Models)
                                             ↓
                                  [Analytics Ready]
                                     (Mart Models)
                                             ↓
                                    [Quality Tests]
                                   (Reconciliation)
                                             ↓
                                    [Go-Live & Monitor]
                                    (Snapshots + SCD)
```

**Best Practice:**
1. Start with staging (1:1 copy of source)
2. Add intermediate (business logic)
3. Create marts (analytics-ready)
4. Add tests at every layer
5. Version control everything
6. Document continuously
7. Test on dev before prod