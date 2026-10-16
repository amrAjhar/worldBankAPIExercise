### Overview

An end-to-end data engineering pipeline built on **Databricks** that extracts human-activity and spatial-trends indicators from the **World Bank API**, processes them through a **medallion architecture** (Bronze → Silver → Gold), and produces business-ready analytical tables for identifying urbanization patterns, density shifts, and infrastructure gaps across 261 countries spanning 1960–2025.

### Architecture

```
World Bank API (3 indicators)
        │
        ▼
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────────┐
│   BRONZE         │────▶│   SILVER         │────▶│   GOLD (3 tables)        │
│   Raw Extract    │     │   Clean + Type  │     │   Analytics + Rankings  │
│   52,668 rows    │     │   51,678 rows   │     │   261 + 214 + 15 rows   │
│   Managed Delta │     │   Managed Delta │     │   Managed Delta + Z-Order│
└──────────────────┘     └──────────────────┘     └──────────────────────────┘
```

**Orchestrated as a 3-task Databricks Job** with dependency chain: Bronze → Silver → Gold. Each task is a notebook running on serverless compute.

### Indicators Extracted

| Code | Indicator | Use Case |
| --- | --- | --- |
| `EN.POP.DNST` | Population Density (people/sq. km) | Tracks land crowding over time |
| `SP.URB.TOTL.IN.ZS` | Urban Population (% of total) | Measures rural-to-city migration |
| `EG.ELC.ACCS.ZS` | Access to Electricity (% of population) | Maps infrastructure spread |

### Layer Details

**Bronze** (`02_Bronze_WorldBank_Extract`)
- Custom `WorldBankAPIClient` class with metadata-driven pagination (reads total page count from API response, loops `page=1..N` per indicator)
- No authentication required — the World Bank API is fully public
- Pandas memory bridge: API JSON → `pandas.DataFrame` → `spark.createDataFrame` → `saveAsTable` (serverless-compliant, no DBFS)
- Write mode: `overwrite` (fresh extract each run, no duplicates)
- Output: 52,668 rows across 262 countries × 3 indicators

**Silver** (`03_Silver_WorldBank_Transform`)
- Flattens and deduplicates on composite key `(country_iso3, year, indicator_code)`
- Strict schema enforcement via `StructType` with `DoubleType` for indicator values (densities, percentages)
- Filters out World Bank income-level aggregates (rows with empty `country_iso3`)
- Delta `MERGE` (upsert) with schema-evolution support for incremental re-runs
- Output: 51,678 rows, 261 countries (990 aggregate rows removed)

**Gold** (`04_Gold_WorldBank_Aggregate`)
Three analytical tables built with **Spark window functions**:

| Table | Method | Rows | Top Finding |
| --- | --- | --- | --- |
| `urbanization_velocity` | `lag(value, 5)` — 5-year rolling growth rate of urban %, ranked descending | 261 | Burundi: +28.88% 5yr urban growth |
| `density_spike` | Absolute density change 2014→2024 (2025 API values are NULL), ranked | 214 | Monaco: +713.56 people/km² |
| `vulnerable_urbanization` | Cross-indicator join: urban YoY growth >1% AND electricity YoY growth <0.5% | 15 | Mali: cities growing +1.91% while electricity access *declining* -9.17% |

### Key Technical Decisions

* **Serverless compute compatibility** — All I/O uses `saveAsTable` (managed Delta) and `spark.table()`, avoiding DBFS paths entirely
* **Metadata-driven pagination** — Unlike cursor/next-URL pagination, the World Bank API reports total page count in response metadata; the client loops through pages per indicator
* **Composite merge key** — Silver layer deduplicates on `(country_iso3, year, indicator_code)` to support multi-indicator data in a single table
* **NULL handling** — The API returns 2025 as a year with NULL values for some indicators; the gold layer filters nulls before computing lag-based growth rates
* **Z-Order optimization** — Gold tables are Z-Ordered on `country_iso3` for query performance

### Tech Stack

| Category | Technology |
| --- | --- |
| Cloud | AWS (S3 via Unity Catalog) |
| Platform | Databricks (Serverless / Spark Connect) |
| Processing | PySpark, Delta Lake |
| Storage | Unity Catalog managed Delta tables |
| Orchestration | Databricks Jobs (multi-task, daily schedule) |
| Data Source | World Bank API v2 (REST, JSON, no auth) |
| Languages | Python, PySpark SQL |

### Data Flow Summary

```
52,668 raw rows (Bronze)
    → 990 aggregate rows filtered
    → 51,678 clean rows (Silver)
    → 261 urbanization velocity rankings
    → 214 density spike rankings
    → 15 vulnerable urbanization flags (Gold)
```

### Pipeline Run Proof

Latest successful run: `SUCCESS` — all 3 tasks passed on first attempt (~2 min total). Full job config and run metadata saved in `databricks_workflow.json`.
