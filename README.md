# Koinz Data Engineering Case Study

## Overview

ETL pipeline that syncs data from PostgreSQL to ClickHouse every 30 minutes using Apache Spark.

## Problem

Transfer new and updated records from `app_user_visits_fact` table:
- **Source**: PostgreSQL (OLTP)
- **Destination**: ClickHouse (OLAP)
- **Method**: Incremental loading based on `updated_at` timestamp

## Solution

```
PostgreSQL → Spark (reads WHERE updated_at > checkpoint) → ClickHouse
```

The pipeline:
1. Reads last checkpoint timestamp
2. Fetches only new/updated records from PostgreSQL
3. Writes to ClickHouse
4. Updates checkpoint

## Project Files

- `spark_app.py` - Main Spark application
- `clickhouse_schema.sql` - ClickHouse table DDL
- `sample_data.sql` - Sample data for reference
- `README.md` - This file


```

## Key Design Decisions

**Why `updated_at` instead of `created_at`?**  
Records can be updated after creation (points spent, status changes). Using `updated_at` ensures we capture all changes.

**Why ReplacingMergeTree?**  
Automatically handles duplicates by keeping the latest version based on `updated_at`.

**Why Batch (30 min) instead of Streaming?**  
Simpler architecture, meets requirements, sufficient for analytics use case.

## Requirements

- Python 3.8+
- Apache Spark 3.5+
- PostgreSQL JDBC Driver
- ClickHouse JDBC Driver

---

**Author**: Ahmed Mohamed El-Slayed  
**Contact**: shabaik1996@gmail.com