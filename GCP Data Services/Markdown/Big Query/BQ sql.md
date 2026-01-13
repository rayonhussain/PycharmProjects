# BigQuery SQL Queries in Data Engineering Lifecycle

## 1. Data Ingestion & Loading

### Creating External Tables
```sql
CREATE OR REPLACE EXTERNAL TABLE `project.dataset.external_gcs_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://my-bucket/data/*.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);
```
**Use Case:** Read data directly from Cloud Storage without copying; cost-effective for large datasets; quick exploration of raw data.

### Loading Data from Cloud Storage
```sql
LOAD DATA INTO `project.dataset.raw_data`
FROM FILES (
  format = 'CSV',
  uris = ['gs://my-bucket/batch_load_*.csv'],
  skip_leading_rows = 1
);
```
**Use Case:** Batch loading structured data; efficient for scheduled data pipelines; supports multiple file patterns.

### Ingesting from Dataflow/Pub/Sub
```sql
CREATE OR REPLACE TABLE `project.dataset.streaming_events` AS
SELECT 
  PARSE_JSON(data) as event_data,
  publish_time,
  message_id
FROM `project.dataset.pubsub_subscription`
WHERE publish_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);
```
**Use Case:** Real-time event ingestion; streaming data processing; maintaining event history.

---

## 2. Data Exploration & Quality Checks

### Schema Discovery with NULL Analysis
```sql
SELECT
  column_name,
  COUNT(*) as total_rows,
  COUNTIF(column_value IS NULL) as null_count,
  ROUND(COUNTIF(column_value IS NULL) / COUNT(*) * 100, 2) as null_percentage,
  APPROX_QUANTILES(CAST(column_value as INT64), 100)[OFFSET(50)] as median_value
FROM UNNEST(STRUCT(col1, col2, col3) as (column_name, column_value))
GROUP BY column_name;
```
**Use Case:** Data profiling; identifying missing values; detecting data quality issues early.

### Duplicate Detection
```sql
WITH row_counts AS (
  SELECT 
    customer_id,
    order_id,
    COUNT(*) as occurrence
  FROM `project.dataset.orders`
  GROUP BY customer_id, order_id
)
SELECT *
FROM row_counts
WHERE occurrence > 1
ORDER BY occurrence DESC;
```
**Use Case:** Data deduplication validation; identifying corrupt records; quality assurance.

### Outlier Detection with Statistical Methods
```sql
WITH stats AS (
  SELECT
    AVG(amount) as mean_amount,
    STDDEV(amount) as stddev_amount
  FROM `project.dataset.transactions`
  WHERE amount IS NOT NULL
)
SELECT
  transaction_id,
  amount,
  ROUND((amount - stats.mean_amount) / stats.stddev_amount, 2) as z_score
FROM `project.dataset.transactions`, stats
WHERE ABS((amount - stats.mean_amount) / stats.stddev_amount) > 3;
```
**Use Case:** Anomaly detection; fraud identification; data quality monitoring.

---

## 3. Data Transformation & Cleaning

### Type Casting & Format Conversion
```sql
CREATE OR REPLACE TABLE `project.dataset.cleaned_data` AS
SELECT
  SAFE_CAST(order_id as INT64) as order_id,
  PARSE_DATE('%Y-%m-%d', order_date) as order_date,
  UPPER(TRIM(customer_name)) as customer_name,
  ROUND(CAST(price as FLOAT64), 2) as price,
  REGEXP_REPLACE(phone, '[^0-9]', '') as phone_cleaned
FROM `project.dataset.raw_data`;
```
**Use Case:** Data standardization; type conversion; format normalization for downstream processing.

### Null Handling & Missing Value Imputation
```sql
SELECT
  order_id,
  COALESCE(customer_id, -1) as customer_id,
  CASE 
    WHEN country IS NULL THEN 'Unknown'
    ELSE country 
  END as country,
  IFNULL(discount_rate, 0) as discount_rate,
  IF(quantity IS NULL, LAG(quantity) OVER (PARTITION BY product_id ORDER BY order_date), quantity) as quantity_imputed
FROM `project.dataset.orders`
ORDER BY order_id;
```
**Use Case:** Handling missing values; forward/backward fill in time series; imputation strategies.

### String Manipulation & Validation
```sql
SELECT
  email,
  REGEXP_EXTRACT(email, r'^([a-zA-Z0-9._%+-]+)@') as username,
  REGEXP_EXTRACT(email, r'@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$') as domain,
  LENGTH(email) as email_length,
  CASE 
    WHEN REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') THEN 'Valid'
    ELSE 'Invalid'
  END as email_validation
FROM `project.dataset.customers`;
```
**Use Case:** Email/phone validation; data extraction; pattern matching.

### Date/Time Transformations
```sql
SELECT
  order_id,
  order_timestamp,
  CAST(order_timestamp AS DATE) as order_date,
  EXTRACT(YEAR FROM order_timestamp) as year,
  EXTRACT(MONTH FROM order_timestamp) as month,
  EXTRACT(QUARTER FROM order_timestamp) as quarter,
  FORMAT_DATE('%A', CAST(order_timestamp AS DATE)) as day_of_week,
  TIMESTAMP_TRUNC(order_timestamp, HOUR) as hour_bucket,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), order_timestamp, DAY) as days_ago
FROM `project.dataset.orders`;
```
**Use Case:** Temporal analysis; bucketing for aggregations; time-based filtering.

### Pivoting & Unpivoting Data
```sql
SELECT
  customer_id,
  SUM(CASE WHEN month = 'January' THEN amount ELSE 0 END) as jan_sales,
  SUM(CASE WHEN month = 'February' THEN amount ELSE 0 END) as feb_sales,
  SUM(CASE WHEN month = 'March' THEN amount ELSE 0 END) as mar_sales
FROM `project.dataset.sales`
GROUP BY customer_id;
```
**Use Case:** Creating cross-tab reports; reshaping wide data; analytical reporting.

---

## 4. Data Enrichment & Joining

### Complex Multi-table Joins
```sql
SELECT
  o.order_id,
  c.customer_name,
  p.product_name,
  o.quantity,
  p.unit_price,
  o.quantity * p.unit_price as total_amount,
  s.status,
  s.shipped_date
FROM `project.dataset.orders` o
INNER JOIN `project.dataset.customers` c ON o.customer_id = c.customer_id
INNER JOIN `project.dataset.products` p ON o.product_id = p.product_id
LEFT JOIN `project.dataset.shipments` s ON o.order_id = s.order_id
WHERE o.order_date >= '2024-01-01'
  AND c.country = 'USA';
```
**Use Case:** Creating denormalized fact tables; multi-source data enrichment; building analytical datasets.

### Temporal Joins (Effective Dating)
```sql
SELECT
  t.transaction_id,
  t.customer_id,
  t.transaction_date,
  cr.rate,
  t.amount * cr.rate as usd_amount
FROM `project.dataset.transactions` t
LEFT JOIN `project.dataset.currency_rates` cr
  ON t.currency_code = cr.currency_code
  AND t.transaction_date >= cr.effective_date
  AND t.transaction_date < cr.end_date;
```
**Use Case:** Dimension table lookups with validity dates; slowly changing dimensions; historical lookups.

### Fuzzy Matching
```sql
SELECT
  a.customer_id,
  b.customer_id as matched_customer_id,
  a.customer_name,
  b.customer_name as matched_name,
  1 - (EDIT_DISTANCE(a.customer_name, b.customer_name) / LENGTH(a.customer_name)) as similarity_score
FROM `project.dataset.customers_new` a
CROSS JOIN `project.dataset.customers_master` b
WHERE ABS(LENGTH(a.customer_name) - LENGTH(b.customer_name)) <= 5
  AND 1 - (EDIT_DISTANCE(a.customer_name, b.customer_name) / LENGTH(a.customer_name)) > 0.85;
```
**Use Case:** Duplicate customer resolution; record linkage; data matching.

---

## 5. Aggregation & Summarization

### Time Series Aggregation
```sql
SELECT
  DATE_TRUNC(order_date, WEEK) as week_start,
  product_category,
  COUNT(DISTINCT order_id) as order_count,
  SUM(amount) as total_sales,
  AVG(amount) as avg_order_value,
  STDDEV(amount) as sales_stddev
FROM `project.dataset.orders`
GROUP BY week_start, product_category
ORDER BY week_start DESC, total_sales DESC;
```
**Use Case:** Revenue reporting; trend analysis; periodic business metrics.

### Hierarchical Aggregation
```sql
SELECT
  country,
  state,
  city,
  COUNT(DISTINCT customer_id) as customer_count,
  SUM(total_spent) as total_revenue
FROM `project.dataset.customer_locations`
WHERE order_date >= '2024-01-01'
GROUP BY ROLLUP(country, state, city)
ORDER BY country, state, city;
```
**Use Case:** Multi-level reporting; hierarchy analysis; drill-down analytics.

### Window Functions with Running Totals
```sql
SELECT
  date,
  product_id,
  sales,
  SUM(sales) OVER (PARTITION BY product_id ORDER BY date) as cumulative_sales,
  LAG(sales) OVER (PARTITION BY product_id ORDER BY date) as previous_day_sales,
  LEAD(sales) OVER (PARTITION BY product_id ORDER BY date) as next_day_sales,
  ROUND(100 * (sales - LAG(sales) OVER (PARTITION BY product_id ORDER BY date)) / LAG(sales) OVER (PARTITION BY product_id ORDER BY date), 2) as pct_change
FROM `project.dataset.daily_sales`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY product_id, date;
```
**Use Case:** YoY comparisons; growth rate calculations; trend analysis.

### Ranking & Percentile Analysis
```sql
SELECT
  customer_id,
  order_date,
  amount,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_sequence,
  RANK() OVER (ORDER BY amount DESC) as amount_rank,
  PERCENT_RANK() OVER (ORDER BY amount) as percentile,
  NTILE(4) OVER (ORDER BY amount DESC) as quartile
FROM `project.dataset.orders`;
```
**Use Case:** Customer segmentation; RFM analysis; percentile-based filtering.

### Distinct Count with HyperLogLog
```sql
SELECT
  DATE(access_time) as date,
  endpoint,
  COUNT(DISTINCT user_id) as unique_users,
  APPROX_COUNT_DISTINCT(user_id) as approx_unique_users,
  COUNT(*) as total_requests
FROM `project.dataset.api_logs`
GROUP BY date, endpoint
ORDER BY date DESC;
```
**Use Case:** User metrics calculation; approximate counting for large datasets; performance optimization.

---

## 6. Deduplication & Record Consolidation

### Row Deduplication (Latest Record)
```sql
WITH ranked_records AS (
  SELECT
    customer_id,
    order_id,
    amount,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) as rn
  FROM `project.dataset.orders`
)
SELECT * EXCEPT(rn)
FROM ranked_records
WHERE rn = 1;
```
**Use Case:** Handling late-arriving updates; SCD Type 1 implementation; maintaining current state.

### Deduplication with Aggregation
```sql
SELECT
  DISTINCT
  customer_id,
  FIRST_VALUE(customer_name) OVER (PARTITION BY customer_id ORDER BY last_updated DESC) as customer_name,
  FIRST_VALUE(email) OVER (PARTITION BY customer_id ORDER BY last_updated DESC) as email,
  COUNT(*) OVER (PARTITION BY customer_id) as record_count
FROM `project.dataset.customer_staging`;
```
**Use Case:** Data consolidation; handling duplicates with aggregation; golden record creation.

---

## 7. Feature Engineering

### Behavioral Aggregation
```sql
SELECT
  customer_id,
  COUNT(DISTINCT order_id) as total_orders,
  COUNT(DISTINCT DATE(order_date)) as days_with_purchases,
  SUM(amount) as lifetime_value,
  AVG(amount) as avg_order_value,
  MAX(order_date) as last_purchase_date,
  DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) as days_since_last_purchase,
  MIN(order_date) as first_purchase_date,
  DATE_DIFF(MAX(order_date), MIN(order_date), DAY) as customer_tenure_days
FROM `project.dataset.orders`
GROUP BY customer_id;
```
**Use Case:** Customer segmentation; churn prediction; RFM modeling.

### Sequence Pattern Detection
```sql
SELECT
  user_id,
  STRING_AGG(event_type ORDER BY event_timestamp LIMIT 5) as event_sequence,
  COUNT(*) as event_count,
  TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND) as session_duration_seconds
FROM `project.dataset.user_events`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY user_id
HAVING COUNT(*) > 5;
```
**Use Case:** User journey analysis; funnel tracking; session analysis.

### Categorical Encoding
```sql
SELECT
  order_id,
  CASE 
    WHEN amount < 50 THEN 'Low'
    WHEN amount < 200 THEN 'Medium'
    WHEN amount < 500 THEN 'High'
    ELSE 'Premium'
  END as amount_category,
  CASE 
    WHEN EXTRACT(HOUR FROM order_timestamp) >= 6 AND EXTRACT(HOUR FROM order_timestamp) < 12 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM order_timestamp) >= 12 AND EXTRACT(HOUR FROM order_timestamp) < 18 THEN 'Afternoon'
    ELSE 'Evening'
  END as time_of_day
FROM `project.dataset.orders`;
```
**Use Case:** Discretization for ML models; categorical feature creation; binning.

---

## 8. Incremental Processing & CDC

### Incremental Data Load (Change Data Capture)
```sql
MERGE `project.dataset.customer_master` t
USING (
  SELECT
    customer_id,
    customer_name,
    email,
    last_updated,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated DESC) as rn
  FROM `project.dataset.customer_staging`
  WHERE last_updated > (SELECT MAX(last_updated) FROM `project.dataset.customer_master`)
) s
ON t.customer_id = s.customer_id AND s.rn = 1
WHEN MATCHED AND s.last_updated > t.last_updated THEN
  UPDATE SET
    t.customer_name = s.customer_name,
    t.email = s.email,
    t.last_updated = s.last_updated
WHEN NOT MATCHED THEN
  INSERT (customer_id, customer_name, email, last_updated)
  VALUES (s.customer_id, s.customer_name, s.email, s.last_updated);
```
**Use Case:** Incremental dimension updates; slowly changing dimensions; efficient data sync.

### Delete Detection with CTE
```sql
CREATE OR REPLACE TABLE `project.dataset.customer_current` AS
WITH deleted_records AS (
  SELECT
    customer_id,
    TRUE as is_deleted
  FROM `project.dataset.customer_staging`
  WHERE deleted_flag = 1
)
SELECT
  c.*,
  COALESCE(d.is_deleted, FALSE) as is_deleted
FROM `project.dataset.customer_master` c
LEFT JOIN deleted_records d USING (customer_id)
WHERE COALESCE(d.is_deleted, FALSE) = FALSE;
```
**Use Case:** Soft deletes; deletion tracking; maintaining data history.

---

## 9. Data Validation & Quality Gates

### Schema Validation
```sql
SELECT
  STRUCT_EXTRACT_ALL(PARSE_JSON(raw_data)) as extracted_fields,
  COUNT(*) as count
FROM `project.dataset.raw_events`
WHERE 
  JSON_EXTRACT_SCALAR(raw_data, '$.event_id') IS NOT NULL
  AND JSON_EXTRACT_SCALAR(raw_data, '$.timestamp') IS NOT NULL
  AND SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', JSON_EXTRACT_SCALAR(raw_data, '$.timestamp')) IS NOT NULL
GROUP BY extracted_fields;
```
**Use Case:** JSON schema validation; required field checking; data format validation.

### Referential Integrity Checks
```sql
SELECT
  'Missing customer references' as issue,
  COUNT(*) as count
FROM `project.dataset.orders` o
WHERE NOT EXISTS (
  SELECT 1 FROM `project.dataset.customers` c WHERE c.customer_id = o.customer_id
)
UNION ALL
SELECT
  'Orphaned customer records' as issue,
  COUNT(*) as count
FROM `project.dataset.customers` c
WHERE NOT EXISTS (
  SELECT 1 FROM `project.dataset.orders` o WHERE o.customer_id = c.customer_id
);
```
**Use Case:** Foreign key validation; data integrity checks; orphaned record detection.

### Data Freshness Checks
```sql
SELECT
  table_name,
  MAX(load_timestamp) as last_load_time,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(load_timestamp), HOUR) as hours_since_load,
  CASE 
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(load_timestamp), HOUR) > 24 THEN 'Stale'
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(load_timestamp), HOUR) > 6 THEN 'Warning'
    ELSE 'Fresh'
  END as freshness_status
FROM `project.dataset.table_metadata`
GROUP BY table_name;
```
**Use Case:** Data pipeline monitoring; SLA tracking; freshness alerts.

---

## 10. Materialized Views & Snapshots

### Creating Materialized Views
```sql
CREATE MATERIALIZED VIEW `project.dataset.customer_summary_mv` AS
SELECT
  customer_id,
  COUNT(DISTINCT order_id) as total_orders,
  SUM(amount) as total_spent,
  AVG(amount) as avg_order_value,
  MAX(order_date) as last_order_date
FROM `project.dataset.orders`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
GROUP BY customer_id;
```
**Use Case:** Query performance optimization; pre-aggregated metrics; reducing computational overhead.

### Snapshot & Historical Tracking
```sql
CREATE OR REPLACE TABLE `project.dataset.customer_snapshot_2024_01_01` AS
SELECT
  *,
  '2024-01-01' as snapshot_date,
  CURRENT_TIMESTAMP() as created_at
FROM `project.dataset.customer_current`
WHERE is_deleted = FALSE;
```
**Use Case:** Point-in-time analysis; regulatory compliance; audit trails.

---

## 11. Advanced Transformations

### Recursive CTEs for Hierarchies
```sql
WITH RECURSIVE employee_hierarchy AS (
  SELECT
    employee_id,
    employee_name,
    manager_id,
    0 as level,
    CAST(employee_id as STRING) as hierarchy_path
  FROM `project.dataset.employees`
  WHERE manager_id IS NULL
  
  UNION ALL
  
  SELECT
    e.employee_id,
    e.employee_name,
    e.manager_id,
    h.level + 1,
    CONCAT(h.hierarchy_path, '->', e.employee_id)
  FROM `project.dataset.employees` e
  INNER JOIN employee_hierarchy h ON e.manager_id = h.employee_id
  WHERE h.level < 10
)
SELECT * FROM employee_hierarchy
ORDER BY hierarchy_path;
```
**Use Case:** Organizational hierarchy analysis; tree structures; recursive relationships.

### Array & Struct Operations
```sql
SELECT
  order_id,
  ARRAY_LENGTH(items) as item_count,
  ARRAY_AGG(STRUCT(item_id, quantity, unit_price)) as order_items,
  (SELECT AS STRUCT 
    SUM(quantity * unit_price) as total_amount,
    AVG(unit_price) as avg_price
  FROM UNNEST(items)) as order_summary
FROM `project.dataset.orders`
GROUP BY order_id;
```
**Use Case:** Complex nested data handling; denormalization; JSON-like structures.

### Machine Learning Feature Preparation
```sql
SELECT
  customer_id,
  CAST(total_orders as FLOAT64) as feature_order_count,
  LOG10(CAST(lifetime_value as FLOAT64)) as feature_log_ltv,
  SAFE_DIVIDE(total_orders, days_since_first_purchase) as feature_order_frequency,
  SAFE_DIVIDE(lifetime_value, total_orders) as feature_avg_order_value,
  ML.STANDARD_SCALER(CAST(days_since_last_purchase as FLOAT64)) OVER () as feature_days_since_purchase_scaled
FROM `project.dataset.customer_features`;
```
**Use Case:** ML model preparation; feature normalization; BigQuery ML integration.

---

## 12. Export & Distribution

### Export to GCS
```sql
EXPORT DATA OPTIONS(
  uri='gs://my-bucket/exports/customers_*.csv',
  format='CSV',
  overwrite=false,
  header=true,
  field_delimiter=','
) AS
SELECT
  customer_id,
  customer_name,
  email,
  total_spent
FROM `project.dataset.customer_summary`
WHERE total_spent > 0;
```
**Use Case:** Data distribution; external system integration; periodic exports.

### Export to Parquet for Analytics Tools
```sql
EXPORT DATA OPTIONS(
  uri='gs://my-bucket/analytics/orders_*.parquet',
  format='PARQUET',
  overwrite=true
) AS
SELECT * FROM `project.dataset.orders`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
```
**Use Case:** BI tool ingestion; Tableau/Looker integration; optimized file format.

---

## 13. Data Lineage & Metadata Management

### Capturing Data Lineage
```sql
CREATE OR REPLACE TABLE `project.dataset.data_lineage_log` AS
SELECT
  CURRENT_TIMESTAMP() as load_time,
  'customer_summary' as target_table,
  'orders, customers' as source_tables,
  (SELECT COUNT(*) FROM `project.dataset.orders`) as source_record_count,
  (SELECT COUNT(*) FROM `project.dataset.customer_summary`) as target_record_count,
  'Incremental load from staging' as process_description;
```
**Use Case:** Metadata tracking; data governance; lineage documentation.

---

## 14. Performance Optimization Queries

### Query Optimization with Clustering
```sql
CREATE OR REPLACE TABLE `project.dataset.orders_clustered`
CLUSTER BY customer_id, order_date AS
SELECT * FROM `project.dataset.orders`;
```
**Use Case:** Query performance improvement; reducing full table scans; cost optimization.

### Partitioning Strategy
```sql
CREATE OR REPLACE TABLE `project.dataset.events_partitioned`
PARTITION BY DATE(event_timestamp)
CLUSTER BY user_id, event_type AS
SELECT
  user_id,
  event_type,
  event_timestamp,
  event_data
FROM `project.dataset.raw_events`;
```
**Use Case:** Query speed optimization; cost reduction; time-based filtering efficiency.

---

## 15. Error Handling & Monitoring

### Safe Type Conversions
```sql
SELECT
  order_id,
  SAFE_CAST(amount as FLOAT64) as amount_safe,
  SAFE.PARSE_DATE('%Y-%m-%d', order_date) as parsed_date,
  SAFE_DIVIDE(total_amount, quantity) as unit_price_safe,
  IF(SAFE_CAST(amount as FLOAT64) IS NULL, 'Conversion failed', 'Success') as conversion_status
FROM `project.dataset.orders`;
```
**Use Case:** Preventing query failures; handling malformed data; graceful error management.

### Error Logging
```sql
CREATE OR REPLACE TABLE `project.dataset.error_log` AS
SELECT
  CURRENT_TIMESTAMP() as error_time,
  'Customer load' as process_name,
  'Invalid email format' as error_message,
  COUNT(*) as error_count
FROM `project.dataset.customer_staging`
WHERE NOT REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
GROUP BY process_name, error_message;
```
**Use Case:** Pipeline error tracking; data quality monitoring; alerting.

---

## 16. Complex Business Logic

### RFM (Recency, Frequency, Monetary) Analysis
```sql
WITH customer_metrics AS (
  SELECT
    customer_id,
    DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) as recency_days,
    COUNT(DISTINCT order_id) as frequency_count,
    SUM(amount) as monetary_value
  FROM `project.dataset.orders`
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  GROUP BY customer_id
)
SELECT
  customer_id,
  recency_days,
  frequency_count,
  monetary_value,
  NTILE(5) OVER (ORDER BY recency_days DESC) as recency_score,
  NTILE(5) OVER (ORDER BY frequency_count) as frequency_score,
  NTILE(5) OVER (ORDER BY monetary_value) as monetary_score,
  CASE
    WHEN NTILE(5) OVER (ORDER BY recency_days DESC) >= 4 
      AND NTILE(5) OVER (ORDER BY frequency_count) >= 4 
      AND NTILE(5) OVER (ORDER BY monetary_value) >= 4 THEN 'Champions'
    WHEN NTILE(5) OVER (ORDER BY recency_days DESC) >= 3 THEN 'Loyal Customers'
    ELSE 'At Risk'
  END as customer_segment
FROM customer_metrics;
```
**Use Case:** Customer segmentation; marketing targeting; retention analysis.

### Cohort Analysis
```sql
WITH cohort_data AS (
  SELECT
    customer_id,
    DATE_TRUNC(MIN(order_date), MONTH) as cohort_month,
    DATE_TRUNC(order_date, MONTH) as order_month,
    SUM(amount) as monthly_spend
  FROM `project.dataset.orders`
  GROUP BY customer_id, order_date
)
SELECT
  cohort_month,
  DATE_DIFF(order_month, cohort_month, MONTH) as months_since_first_purchase,
  COUNT(DISTINCT customer_id) as cohort_size,
  SUM(monthly_spend) as cohort_revenue
FROM cohort_data
GROUP BY cohort_month, months_since_first_purchase
ORDER BY cohort_month, months_since_first_purchase;
```
**Use Case:** Retention analysis; cohort tracking; lifetime value analysis.