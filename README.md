# Multi-Model Big Data E-Commerce Analytics Platform

An enterprise-grade, multi-model big data architecture deployed via Docker. The system isolates operational enterprise transactions within an Operational Data Store (ODS) layer using **MongoDB**, while routing high-velocity clickstream web events into **Apache HBase** wide-column storage. Distributed computation, data standardization, and complex analytics integration are executed over the cluster using **Apache Spark (PySpark)**.

---

## System Architecture Deployment

The multi-model database engine layer is orchestrated inside a virtual network via a unified Docker Compose manifest stack.

### 1. Fire up Database Infrastructure
From the repository root, run the following command to spin up MongoDB and HBase concurrently in background mode:
```bash
docker compose up -d
```

### 2. Verify Service Statuses
* **MongoDB Core:** Accessible locally at `mongodb://localhost:27017`
* **HBase Master Web UI Console:** Visible in your web browser at `http://localhost:16010`
* **HBase Thrift Network Connection Gate:** Listens on port `9090` for Python/HappyBase client mappings.

---

## Step-by-Step Execution Guide

Follow these steps sequentially to fully populate the environments, execute the batch processing pipelines, and reproduce the project's analytical benchmarks.

### Step 1: Initialize Wide-Column Schemas (HBase Shell)
Execute straight into your active HBase cluster container to create the wide-column schemas:
```bash
docker exec -it auca-hbase hbase shell
```
Inside the interactive HBase Shell environment, run these commands:
```hbase
create 'user_browsing_data', 'cf_session'
create 'product_performance', 'cf_metrics'
list
exit
```

### Step 2: Stream Time-Series Data Ingestion (HBase)
Install dependencies locally (`pip install happybase`) and run the loading script to ingest your representative data subset. This script applies a composite row key layout of `user_id + (Long.MAX_VALUE - epoch_timestamp)` to automatically sort newest user timeline entries first:
```bash
python ingest_to_hbase.py
```

### Step 3: Run Distributed Data Cleaning & Batch Analytics (Spark)
Process 500,000 raw transaction items, flatten out multi-nested JSON array structures using `explode()`, and run the association rules self-join matrix to find co-purchased patterns ("*Users who bought X also bought Y*"):
```bash
python spark_batch_job.py
```

### Step 4: Execute Multi-Model Cross-Platform Joins (Spark SQL)
Register your isolated MongoDB databases and HBase event streams as shared memory Relational Views (`mongo_users`, `hbase_sessions`) and run standard ANSI-SQL queries across disparate data engines:
```bash
python spark_sql_views.py
```

### Step 5: Generate Integrated Funnel Conversion Reports & Plots
Execute the cross-platform integrated analytics script to map your top-to-bottom customer acquisition funnel across product categories:
```bash
python integrated_funnel.py
```

---

## Sample Reproducible Queries

### 1. Optimised Single-User Prefix Scan Query (HBase)
This script runs a specific row-prefix constraint search to isolate a target customer's browsing data block, avoiding a slow, cluster-wide full table scan:
```python
import happybase
connection = happybase.Connection('localhost', port=9090)
table = connection.table('user_browsing_data')

# Prefix-scan isolates single-user block domains instantly
for key, data in table.scan(row_prefix=b"user_000000"):
    print(f"Row Key: {key.decode()} -> Session: {data[b'cf_session:session_id'].decode()}")
```

### 2. Multi-Model Profile Join Query (Spark SQL)
This query performs an `INNER JOIN` over a MongoDB user lookup master table and an append-only clickstream HBase log stream to analyze regional interaction metrics:
```sql
SELECT 
    u.geo_data.country as country,
    COUNT(s.session_id) as total_sessions,
    ROUND(AVG(s.duration_seconds), 2) as avg_duration_seconds
FROM hbase_sessions s
INNER JOIN mongo_users u ON s.user_id = u.user_id
WHERE u.geo_data.country IS NOT NULL
GROUP BY u.geo_data.country
ORDER BY total_sessions DESC
LIMIT 5;
```

---

## Summary Insights Portfolio
* **Top Market Densities:** Clickstream transaction logs reveal that our most active platform customer engagement hubs emerge from sub-Saharan African regions, led by **Zimbabwe (ZW)** and **Rwanda (RW)**.
* **Funnel Performance Optimization:** Funnel aggregation confirms that **`cat_014`** and **`cat_022`** operate as our peak conversion paths. 
* **Data Sampling Note:** Conversion metrics calculating above 100% in the outputs is a deliberate artifact of big data sampling (evaluating a single 188MB slice of time-series traffic rows against a complete historical transactional dataset).
