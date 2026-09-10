# Rearc Data Quest - Databricks Solution

A production-ready data pipeline that ingests BLS productivity time-series data and US population data, transforms it through Bronze/Silver/Gold medallion architecture, and answers analytical questions using Delta Live Tables on Databricks.

## Overview

This solution implements the [Rearc Data Quest](https://github.com/rearc/data-quest) challenge using modern data engineering best practices on Databricks platform.

### Data Sources
- **BLS Productivity Data**: Time-series data from `https://download.bls.gov/pub/time.series/pr/`
- **US Population Data**: Annual population from DataUSA API

### Questions Answered
1. **Population Statistics (2013-2018)**: Mean and standard deviation of US population
2. **Best Year by Series**: Year with highest total value for each BLS series (with human-readable labels)
3. **BLS + Population Join**: Combined dataset for series `PRS30006032` with population data

## Architecture

### Medallion Architecture (Bronze/Silver/Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                          │
│  • Idempotent downloads (MD5 hash tracking)                     │
│  • Timestamped directories (YYYYMMDD_HHMMSS)                    │
│  • Stores to Unity Catalog Volume                               │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                            │
│  • 6 tables: Raw BLS data, series, lookups, population          │
│  • Auto Loader (cloudFiles) for incremental processing          │
│  • Column mapping enabled (handles TSV header spaces)           │
│  • Append-only (full audit trail)                               │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                            │
│  • 6 tables: Cleaned, deduplicated data                         │
│  • SCD Type 1 (APPLY CHANGES INTO pattern)                      │
│  • Column name normalization, data trimming                     │
│  • Quality checks with @dlt.expect_or_drop                      │
│  • Latest records only per natural key                          │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER                             │
│  • 3 tables: Business-ready aggregations                        │
│  • Dual implementation: PySpark AND Spark SQL                   │
│  • Human-readable labels (sector/class/measure lookups)         │
│  • Answers all 3 analytical questions                           │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Stack
- **Platform**: Databricks (Serverless Compute)
- **Pipeline Framework**: Delta Live Tables (DLT)
- **Storage**: Delta Lake on Unity Catalog
- **Deployment**: Databricks Asset Bundles (DAB)
- **Language**: Python (PySpark + Spark SQL)

## Project Structure

```
mp_rearc_data_quest/
├── src/
│   └── rearc_data_quest/
│       ├── ingestion.py              # Idempotent data download
│       └── dlt_pipeline.py           # DLT Bronze/Silver/Gold
├── notebooks/
│   └── rearc_data_quest/
│       ├── ingestion.ipynb           # Ingestion notebook
│       └── pipeline.ipynb            # DLT pipeline notebook
├── resources/
│   └── rearc_data_quest/
│       └── resources.yml             # DAB configuration
├── databricks.yml                    # DAB bundle definition
├── README.md                         # This file
├── images                            # Test run screenshots
└── PROCESS.md                        # Implementation deep-dive
```

## Setup & Deployment

### Prerequisites
- Databricks workspace (AWS/Azure/GCP)
- Unity Catalog with catalog, schema, and volume created
- Databricks CLI installed and authenticated

### Configuration

Edit `databricks.yml`:
```yaml
variables:
  catalog_name: **********            # Your catalog
  schema_name: **********             # Your schema
  volume_name: *********              # Your volume
```

### Deploy

```bash
# Validate bundle
databricks bundle validate

# Deploy to workspace
databricks bundle deploy -t dev

# Run full workflow (ingestion + DLT pipeline)
databricks bundle run rearc_data_quest_workflow -t dev
```

## Key Features

### 1. Idempotent Ingestion
- **MD5 hash tracking** of file content (not file metadata)
- Skips unchanged files automatically
- Detects added, changed, and removed files
- Separate trackers for BLS and population data

### 2. Incremental Processing
- **Auto Loader** with cloudFiles for streaming ingestion
- Processes only new files in volume
- Schema evolution support

### 3. Data Quality
- **Column name normalization** (handles TSV header spaces)
- **Data trimming** (removes trailing spaces from values)
- **Quality checks** with DLT expectations
- **Deduplication** using SCD Type 1 pattern

### 4. Human-Readable Labels
- Joins with BLS lookup tables (pr.measure, pr.sector, pr.class)
- Constructs descriptions: `"Manufacturing - All workers - Output"`
- Makes data accessible to non-domain experts

### 5. Dual Implementation
- **PySpark** (primary): Programmatic, testable
- **Spark SQL** (alternative): Declarative, readable
- Switch via `use_sql` configuration parameter

## Tables Created

### Bronze (6 tables)
- `bronze_bls_data` - Raw productivity data (pr.data.*)
- `bronze_bls_series` - Series metadata
- `bronze_bls_measure` - Measure code lookup
- `bronze_bls_sector` - Sector code lookup
- `bronze_bls_class` - Class code lookup
- `bronze_population` - Raw population JSON

### Silver (6 tables)
- `silver_bls_data` - Cleaned BLS data
- `silver_bls_series` - Cleaned series metadata
- `silver_bls_measure` - Cleaned measure lookup
- `silver_bls_sector` - Cleaned sector lookup
- `silver_bls_class` - Cleaned class lookup
- `silver_population` - Parsed population data

### Gold (3 tables)
- `gold_population_stats` - Q1: Population mean/stddev 2013-2018
- `gold_best_year_by_series` - Q2: Best year per series with labels
- `gold_bls_population_join` - Q3: BLS + population combined

## Data Refresh

### Manual Refresh
```bash
# Re-run ingestion (downloads only changed files)
databricks bundle run rearc_data_quest_workflow - t dev
```

### Scheduled Refresh
Add schedule to `resources.yml`:
```yaml
jobs:
  rearc_data_quest_workflow:
    schedule:
      quartz_cron_expression: "0 0 2 * * ?"  # Daily at 2 AM
      timezone_id: "America/Los_Angeles"
```

## Monitoring & Observability

- **DLT Event Log**: Tracks data quality metrics, row counts
- **Pipeline Lineage**: Visual DAG in Databricks UI
- **Ingestion Statistics**: Logs files downloaded/skipped/removed

## Technical Highlights

1. **Handles Real-World Data Issues**
   - TSV files with trailing spaces in headers and values
   - Missing series titles (constructs from lookup tables)
   - API response format (single-line JSON for streaming)

2. **Production-Ready Patterns**
   - SCD Type 1 for overwrites (APPLY CHANGES INTO)
   - Delta column mapping for messy sources
   - Serverless compute for auto-scaling
   - Asset bundles for CI/CD deployment

3. **Performance Optimizations**
   - Streaming ingestion (not full scans)
   - Partition pruning with pathGlobFilter
   - Cached checkpoint locations

## Cost Considerations

- **Serverless DLT**: Pay-per-use, no idle cluster costs
- **Idempotent ingestion**: Skips unchanged files (saves compute)
- **Incremental processing**: Processes only new data

## Troubleshooting

### Issue: Column name errors
**Solution**: Delta column mapping enabled in Bronze layer

### Issue: Duplicate records in Silver
**Solution**: Using SCD Type 1 with `apply_changes()` - overwrites on keys

### Issue: Missing series descriptions
**Solution**: Lookup tables (pr.measure/sector/class) joined in Gold layer

## References

- [Rearc Data Quest Challenge](https://github.com/rearc/data-quest)
- [BLS Productivity Data](https://download.bls.gov/pub/time.series/pr/)
- [DataUSA Population API](https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population)
- [Databricks Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## Author

Implementation by Mahesh

For detailed implementation decisions and trade-offs, see [PROCESS.md](PROCESS.md).
