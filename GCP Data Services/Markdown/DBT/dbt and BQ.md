# BigQuery & DBT Data Engineering Pipeline Guide

## Pipeline Architecture Overview

This document outlines a complete data engineering pipeline using BigQuery (BQ) as the data warehouse and DBT (Data Build Tool) for transformations, following the medallion architecture pattern.

```
Source Systems → BQ (Raw Layer) → DBT (Transformation) → BQ (Reporting Layer) → BI Tools
```

---

## 1. Pipeline Flow & Architecture

### 1.1 Data Flow Layers

**Layer 1: Raw/Landing Layer (BigQuery)**
- Tables: `raw_*` or `landing_*`
- Purpose: Store data exactly as received from source systems
- Characteristics: Immutable, append-only, full history

**Layer 2: Staging Layer (DBT)**
- Models: `stg_*`
- Purpose: Light transformations, data type casting, renaming columns
- Characteristics: Cleaned, standardized column names, basic validation

**Layer 3: Intermediate Layer (DBT)**
- Models: `int_*`
- Purpose: Business logic, joins, aggregations, deduplication
- Characteristics: Reusable transformations, not exposed to end users

**Layer 4: Mart/Reporting Layer (DBT → BQ)**
- Models: `mart_*`, `fact_*`, `dim_*`
- Purpose: Final business-ready tables for reporting
- Characteristics: Denormalized, optimized for queries, documented

---

## 2. Detailed Pipeline Workflow

### Step 1: Data Ingestion to BigQuery (Raw Layer)

```sql
-- Example: Loading raw data into BigQuery
-- This can be done via Cloud Storage, Dataflow, or other ingestion tools

-- Raw table structure
CREATE OR REPLACE TABLE `project.raw_dataset.raw_orders` (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    amount NUMERIC,
    status STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);
```

### Step 2: DBT Staging Models

```sql
-- models/staging/stg_orders.sql
-- Purpose: Clean and standardize raw data

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
),

cleaned AS (
    SELECT
        CAST(order_id AS INT64) AS order_id,
        CAST(customer_id AS INT64) AS customer_id,
        CAST(order_date AS TIMESTAMP) AS order_date,
        CAST(amount AS NUMERIC) AS order_amount,
        LOWER(TRIM(status)) AS order_status,
        _loaded_at,
        _source_file
    FROM source
    WHERE order_id IS NOT NULL
        AND customer_id IS NOT NULL
)

SELECT * FROM cleaned
```

### Step 3: DBT Intermediate Models

```sql
-- models/intermediate/int_orders_enriched.sql
-- Purpose: Apply business logic and join multiple sources

{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

enriched AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_name,
        c.customer_email,
        o.order_date,
        o.order_amount,
        o.order_status,
        CASE
            WHEN o.order_amount >= 1000 THEN 'high_value'
            WHEN o.order_amount >= 500 THEN 'medium_value'
            ELSE 'low_value'
        END AS order_value_segment,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id 
            ORDER BY o.order_date
        ) AS customer_order_number
    FROM orders o
    LEFT JOIN customers c
        ON o.customer_id = c.customer_id
)

SELECT * FROM enriched
```

### Step 4: DBT Mart/Reporting Models

```sql
-- models/marts/mart_daily_orders_summary.sql
-- Purpose: Create aggregated reporting table

{{
    config(
        materialized='incremental',
        unique_key='report_date',
        schema='marts',
        partition_by={
            "field": "report_date",
            "data_type": "date"
        }
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
    WHERE order_date >= (SELECT MAX(report_date) FROM {{ this }})
    {% endif %}
),

daily_summary AS (
    SELECT
        DATE(order_date) AS report_date,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(order_amount) AS total_revenue,
        AVG(order_amount) AS avg_order_value,
        COUNT(DISTINCT CASE WHEN order_status = 'completed' THEN order_id END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN order_status = 'cancelled' THEN order_id END) AS cancelled_orders,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM orders
    GROUP BY 1
)

SELECT * FROM daily_summary
```

---

## 3. DBT Project Structure

```
dbt_project/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── _staging.yml
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   └── stg_products.sql
│   ├── intermediate/
│   │   ├── _intermediate.yml
│   │   ├── int_orders_enriched.sql
│   │   └── int_customer_lifetime_value.sql
│   └── marts/
│       ├── _marts.yml
│       ├── mart_daily_orders_summary.sql
│       └── mart_customer_cohorts.sql
├── tests/
│   ├── generic/
│   │   └── test_record_count_match.sql
│   └── singular/
│       └── test_orders_revenue_reconciliation.sql
└── macros/
    └── compare_record_counts.sql
```

---

## 4. Testing Framework

### 4.1 DBT Generic Tests (Schema Tests)

```yaml
# models/staging/_staging.yml
version: 2

models:
  - name: stg_orders
    description: "Staging table for orders"
    columns:
      - name: order_id
        description: "Unique order identifier"
        tests:
          - unique
          - not_null
      
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      
      - name: order_amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000000
      
      - name: order_status
        tests:
          - accepted_values:
              values: ['pending', 'completed', 'cancelled', 'refunded']
```

### 4.2 Unit Tests for Data Validation

```sql
-- tests/singular/test_orders_no_duplicates.sql
-- Test for duplicate orders

SELECT
    order_id,
    COUNT(*) AS duplicate_count
FROM {{ ref('stg_orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1
```

```sql
-- tests/singular/test_orders_date_validity.sql
-- Test for invalid date ranges

SELECT *
FROM {{ ref('stg_orders') }}
WHERE order_date < '2020-01-01'
    OR order_date > CURRENT_TIMESTAMP()
```

```sql
-- tests/singular/test_orders_amount_not_negative.sql
-- Test for negative amounts

SELECT *
FROM {{ ref('stg_orders') }}
WHERE order_amount < 0
```

### 4.3 Record Count Discrepancy Tests

```sql
-- tests/singular/test_record_count_staging_vs_raw.sql
-- Compare record counts between raw and staging layers

WITH raw_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ source('raw', 'raw_orders') }}
    WHERE order_id IS NOT NULL
),

staging_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('stg_orders') }}
),

comparison AS (
    SELECT
        r.cnt AS raw_count,
        s.cnt AS staging_count,
        r.cnt - s.cnt AS difference,
        ROUND(ABS(r.cnt - s.cnt) * 100.0 / NULLIF(r.cnt, 0), 2) AS difference_pct
    FROM raw_count r
    CROSS JOIN staging_count s
)

SELECT *
FROM comparison
WHERE ABS(difference_pct) > 5  -- Fail if difference is more than 5%
```

```sql
-- tests/singular/test_orders_revenue_reconciliation.sql
-- Validate revenue calculations match between layers

WITH staging_revenue AS (
    SELECT SUM(order_amount) AS total_revenue
    FROM {{ ref('stg_orders') }}
    WHERE order_status = 'completed'
),

mart_revenue AS (
    SELECT SUM(total_revenue) AS total_revenue
    FROM {{ ref('mart_daily_orders_summary') }}
),

comparison AS (
    SELECT
        s.total_revenue AS staging_total,
        m.total_revenue AS mart_total,
        s.total_revenue - m.total_revenue AS difference
    FROM staging_revenue s
    CROSS JOIN mart_revenue m
)

SELECT *
FROM comparison
WHERE ABS(difference) > 0.01  -- Allow for minimal rounding differences
```

### 4.4 Custom Generic Test for Record Count Matching

```sql
-- tests/generic/test_record_count_match.sql
-- Reusable test to compare record counts between two models

{% test record_count_match(model, compare_model, group_by_column=None, tolerance_pct=0) %}

WITH model_count AS (
    SELECT
        {% if group_by_column %}
        {{ group_by_column }},
        {% endif %}
        COUNT(*) AS model_cnt
    FROM {{ model }}
    {% if group_by_column %}
    GROUP BY {{ group_by_column }}
    {% endif %}
),

compare_count AS (
    SELECT
        {% if group_by_column %}
        {{ group_by_column }},
        {% endif %}
        COUNT(*) AS compare_cnt
    FROM {{ compare_model }}
    {% if group_by_column %}
    GROUP BY {{ group_by_column }}
    {% endif %}
),

comparison AS (
    SELECT
        {% if group_by_column %}
        COALESCE(m.{{ group_by_column }}, c.{{ group_by_column }}) AS {{ group_by_column }},
        {% endif %}
        COALESCE(m.model_cnt, 0) AS model_count,
        COALESCE(c.compare_cnt, 0) AS compare_count,
        COALESCE(m.model_cnt, 0) - COALESCE(c.compare_cnt, 0) AS difference,
        ABS(COALESCE(m.model_cnt, 0) - COALESCE(c.compare_cnt, 0)) * 100.0 / 
            NULLIF(GREATEST(COALESCE(m.model_cnt, 0), COALESCE(c.compare_cnt, 0)), 0) AS difference_pct
    FROM model_count m
    FULL OUTER JOIN compare_count c
        {% if group_by_column %}
        ON m.{{ group_by_column }} = c.{{ group_by_column }}
        {% else %}
        ON TRUE
        {% endif %}
)

SELECT *
FROM comparison
WHERE difference_pct > {{ tolerance_pct }}

{% endtest %}
```

Usage:
```yaml
# models/marts/_marts.yml
version: 2

models:
  - name: mart_daily_orders_summary
    tests:
      - record_count_match:
          compare_model: ref('int_orders_enriched')
          group_by_column: 'DATE(order_date)'
          tolerance_pct: 1
```

### 4.5 Data Quality Monitoring Macro

```sql
-- macros/compare_record_counts.sql
-- Macro to generate detailed record count comparison reports

{% macro compare_record_counts(source_table, target_table, grain_column=None) %}

WITH source_data AS (
    SELECT
        {% if grain_column %}
        {{ grain_column }},
        {% endif %}
        COUNT(*) AS record_count,
        MIN(_loaded_at) AS min_load_time,
        MAX(_loaded_at) AS max_load_time
    FROM {{ source_table }}
    {% if grain_column %}
    GROUP BY {{ grain_column }}
    {% endif %}
),

target_data AS (
    SELECT
        {% if grain_column %}
        {{ grain_column }},
        {% endif %}
        COUNT(*) AS record_count,
        MIN(_dbt_updated_at) AS min_load_time,
        MAX(_dbt_updated_at) AS max_load_time
    FROM {{ target_table }}
    {% if grain_column %}
    GROUP BY {{ grain_column }}
    {% endif %}
),

comparison AS (
    SELECT
        {% if grain_column %}
        COALESCE(s.{{ grain_column }}, t.{{ grain_column }}) AS grain,
        {% endif %}
        COALESCE(s.record_count, 0) AS source_count,
        COALESCE(t.record_count, 0) AS target_count,
        COALESCE(s.record_count, 0) - COALESCE(t.record_count, 0) AS count_difference,
        CASE
            WHEN COALESCE(s.record_count, 0) = 0 THEN NULL
            ELSE ROUND(
                ABS(COALESCE(s.record_count, 0) - COALESCE(t.record_count, 0)) * 100.0 / s.record_count,
                2
            )
        END AS difference_pct,
        CASE
            WHEN COALESCE(s.record_count, 0) = COALESCE(t.record_count, 0) THEN 'MATCH'
            WHEN ABS(COALESCE(s.record_count, 0) - COALESCE(t.record_count, 0)) <= 10 THEN 'MINOR_DIFF'
            WHEN COALESCE(s.record_count, 0) = 0 THEN 'MISSING_SOURCE'
            WHEN COALESCE(t.record_count, 0) = 0 THEN 'MISSING_TARGET'
            ELSE 'MAJOR_DIFF'
        END AS status
    FROM source_data s
    FULL OUTER JOIN target_data t
        {% if grain_column %}
        ON s.{{ grain_column }} = t.{{ grain_column }}
        {% else %}
        ON TRUE
        {% endif %}
)

SELECT * FROM comparison
ORDER BY ABS(count_difference) DESC

{% endmacro %}
```

### 4.6 Discrepancy Detection Tests

```sql
-- tests/singular/test_detect_missing_records_in_pipeline.sql
-- Identify records that exist in source but missing in target

WITH source_keys AS (
    SELECT DISTINCT order_id
    FROM {{ source('raw', 'raw_orders') }}
    WHERE order_id IS NOT NULL
),

target_keys AS (
    SELECT DISTINCT order_id
    FROM {{ ref('mart_daily_orders_summary') }}
),

missing_records AS (
    SELECT s.order_id
    FROM source_keys s
    LEFT JOIN target_keys t
        ON s.order_id = t.order_id
    WHERE t.order_id IS NULL
)

SELECT
    'Missing in final mart' AS issue_type,
    COUNT(*) AS missing_count
FROM missing_records
HAVING COUNT(*) > 0
```

```sql
-- tests/singular/test_orphaned_records_in_target.sql
-- Identify records in target that don't exist in source

WITH source_keys AS (
    SELECT DISTINCT order_id
    FROM {{ source('raw', 'raw_orders') }}
),

target_keys AS (
    SELECT DISTINCT order_id
    FROM {{ ref('stg_orders') }}
),

orphaned_records AS (
    SELECT t.order_id
    FROM target_keys t
    LEFT JOIN source_keys s
        ON t.order_id = s.order_id
    WHERE s.order_id IS NULL
)

SELECT
    'Orphaned records in staging' AS issue_type,
    COUNT(*) AS orphaned_count
FROM orphaned_records
HAVING COUNT(*) > 0
```

---

## 5. BigQuery Native Validation Queries

### 5.1 Cross-Table Record Count Validation

```sql
-- Run in BigQuery Console
-- Compare counts across pipeline layers

WITH layer_counts AS (
    SELECT 'raw' AS layer, COUNT(*) AS record_count
    FROM `project.raw_dataset.raw_orders`
    
    UNION ALL
    
    SELECT 'staging' AS layer, COUNT(*) AS record_count
    FROM `project.staging.stg_orders`
    
    UNION ALL
    
    SELECT 'intermediate' AS layer, COUNT(*) AS record_count
    FROM `project.intermediate.int_orders_enriched`
    
    UNION ALL
    
    SELECT 'mart' AS layer, COUNT(DISTINCT order_id) AS record_count
    FROM `project.marts.mart_daily_orders_summary`
)

SELECT
    layer,
    record_count,
    LAG(record_count) OVER (ORDER BY 
        CASE layer
            WHEN 'raw' THEN 1
            WHEN 'staging' THEN 2
            WHEN 'intermediate' THEN 3
            WHEN 'mart' THEN 4
        END
    ) AS prev_layer_count,
    record_count - LAG(record_count) OVER (ORDER BY 
        CASE layer
            WHEN 'raw' THEN 1
            WHEN 'staging' THEN 2
            WHEN 'intermediate' THEN 3
            WHEN 'mart' THEN 4
        END
    ) AS difference
FROM layer_counts
ORDER BY 
    CASE layer
        WHEN 'raw' THEN 1
        WHEN 'staging' THEN 2
        WHEN 'intermediate' THEN 3
        WHEN 'mart' THEN 4
    END;
```

### 5.2 Daily Record Count Anomaly Detection

```sql
-- Detect unusual daily record count patterns

WITH daily_counts AS (
    SELECT
        DATE(order_date) AS order_date,
        COUNT(*) AS daily_count
    FROM `project.staging.stg_orders`
    GROUP BY 1
),

stats AS (
    SELECT
        AVG(daily_count) AS avg_count,
        STDDEV(daily_count) AS stddev_count
    FROM daily_counts
)

SELECT
    d.order_date,
    d.daily_count,
    s.avg_count,
    s.stddev_count,
    (d.daily_count - s.avg_count) / NULLIF(s.stddev_count, 0) AS z_score,
    CASE
        WHEN ABS((d.daily_count - s.avg_count) / NULLIF(s.stddev_count, 0)) > 3 THEN 'ANOMALY'
        WHEN ABS((d.daily_count - s.avg_count) / NULLIF(s.stddev_count, 0)) > 2 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS status
FROM daily_counts d
CROSS JOIN stats s
ORDER BY d.order_date DESC
LIMIT 30;
```

---

## 6. Monitoring & Alerting Strategy

### 6.1 DBT Test Execution

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select stg_orders

# Run specific test type
dbt test --select test_type:singular
dbt test --select test_type:generic

# Run tests with store_failures flag to save failing records
dbt test --store-failures
```

### 6.2 Recommended Monitoring Checks

1. **Row Count Validation**: Daily comparison between expected and actual counts
2. **Data Freshness**: Check `_loaded_at` timestamps in raw tables
3. **Null Rate Monitoring**: Track percentage of nulls in key columns
4. **Duplicate Detection**: Monitor duplicate rates in unique key columns
5. **Revenue Reconciliation**: Cross-check aggregated values across layers
6. **Referential Integrity**: Validate foreign key relationships
7. **Schema Drift Detection**: Alert on unexpected column additions/removals
8. **Processing Time**: Monitor DBT run duration for performance degradation

### 6.3 Alerting Thresholds

```yaml
# Example thresholds for alerts
thresholds:
  record_count_difference_pct: 5      # Alert if >5% difference
  null_rate_critical_columns: 1       # Alert if >1% nulls
  duplicate_rate: 0.1                 # Alert if >0.1% duplicates
  revenue_reconciliation_diff: 100    # Alert if >$100 difference
  data_freshness_hours: 24            # Alert if data >24 hours old
  dbt_run_duration_minutes: 60        # Alert if run takes >60 min
```

---

## 7. Best Practices Summary

1. **Layered Architecture**: Maintain clear separation between raw, staging, intermediate, and mart layers
2. **Incremental Models**: Use incremental materialization for large fact tables
3. **Partitioning**: Leverage BigQuery partitioning on date columns for performance
4. **Documentation**: Document all models, columns, and business logic in YAML files
5. **Version Control**: Keep all DBT code in Git with proper branching strategy
6. **CI/CD Integration**: Automate DBT runs and tests in your deployment pipeline
7. **Data Quality Gates**: Implement comprehensive testing before promoting to production
8. **Monitoring**: Set up automated alerts for data quality issues and pipeline failures
9. **Idempotency**: Ensure DBT models can be re-run without causing data issues
10. **Performance**: Optimize SQL queries and leverage BigQuery's capabilities (clustering, partitioning)

---

## 8. Troubleshooting Common Issues

### Issue: Record counts don't match between layers
- Check for NULL values in join keys
- Verify filtering conditions in WHERE clauses
- Look for duplicate records in source data
- Review incremental logic for missing date ranges

### Issue: Tests failing intermittently
- Check for data type mismatches
- Verify timezone handling in timestamp comparisons
- Review concurrency issues in incremental runs
- Check for schema evolution in source tables

### Issue: Poor query performance
- Add partitioning and clustering to large tables
- Review JOIN order and conditions
- Use materialized tables instead of views for complex logic
- Implement incremental strategies for large datasets

---

## 9. Execution Checklist

- [ ] Raw data loaded into BigQuery landing zone
- [ ] DBT sources configured in `sources.yml`
- [ ] Staging models created with basic transformations
- [ ] Schema tests defined for all staging models
- [ ] Intermediate models built with business logic
- [ ] Mart models created for reporting layer
- [ ] All singular tests passing
- [ ] Record count reconciliation tests in place
- [ ] Documentation complete for all models
- [ ] Monitoring and alerting configured
- [ ] CI/CD pipeline set up for automated runs
- [ ] Stakeholders notified of data availability