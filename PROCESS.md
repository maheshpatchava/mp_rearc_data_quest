# Implementation Process

## Architecture Decisions

### Why Bronze/Silver/Gold (Medallion Architecture)?

**Bronze Layer - Raw Data Preservation:**
- Stores data exactly as received (TSV spaces, raw JSON)
- Append-only for complete audit trail
- Delta column mapping enabled to handle messy TSV headers
- **Why**: Provides reproducibility - can always reprocess Silver from Bronze if logic changes

**Silver Layer - Single Source of Truth:**
- Cleaned column names (strips TSV spaces)
- Trimmed data values
- Deduplicated using SCD Type 1 (keeps only latest per key)
- **Why**: Data quality enforcement happens here, Gold tables don't need to worry about cleaning

**Gold Layer - Business Logic:**
- Aggregations and joins for analytical questions
- Human-readable labels (joins with BLS lookup tables)
- **Why**: Optimized for specific business questions, easy to query

**Alternative Considered:** Flat two-layer (Raw → Curated)
- **Rejected**: Mixing cleaning and business logic makes code harder to maintain

### Why PySpark as Primary, SQL as Alternative?

**PySpark (Primary):**
- More testable (can unit test transformation functions)
- Better for complex transformations (nested JSON parsing)
- IDE support with type hints
- **Trade-off**: More verbose than SQL

**Spark SQL (Alternative):**
- More readable for analysts
- Declarative intent
- **Trade-off**: Harder to test, less IDE support

**Decision**: Implement both, toggle with config parameter
- Developers use PySpark during development
- Analysts can read SQL version for logic review
- Both produce identical results (validated by reading same Silver tables)

### Safe Idempotent Ingestion

**Problem**: BLS files can change, need to avoid reprocessing unchanged data.

**Solution**: MD5 hash tracking
```python
# Track content hash, not file metadata
content_hash = hashlib.md5(file_content).hexdigest()
if tracker.is_file_changed(filename, content_hash):
    download()  # Only if hash changed
```

**Why MD5 over alternatives?**
- File metadata (mtime, size): Unreliable - metadata can change without content change
- Filename tracking: BLS updates same filename with new content
- MD5 hashing: Detects actual content changes

**Re-run Safety:**
- First run: Downloads all files, stores hashes
- Second run: Skips unchanged files (compares hashes)
- Changed files: Downloads only delta
- Removed files: Logs warning but doesn't delete Bronze data (audit trail preserved)

## Trade-offs & Production Considerations

### What I'd Do Differently for Real Client

#### 1. Schema Drift Handling
**Current**: DLT auto-evolves schema
**Production**: 
- Enforce schema at Bronze ingestion
- Add schema version tracking table
- Alert on unexpected new columns
- **Why**: Auto-evolution can hide breaking changes

#### 2. Data Volume Scaling
**Current**: Works well for ~100K BLS records
**Production for 100M+ records**:
- Partition Bronze tables by ingestion date
- Use Z-ordering on frequently joined keys
- Implement incremental Gold table updates (not full refresh)
- Consider compaction strategy for Bronze (retain last N versions only)

#### 3. Cost Optimization
**Current**: Serverless DLT (pay-per-use)
**Production**:
- Schedule jobs during off-peak hours (cheaper DBU rates)
- Use job compute and job pools for concurrency limits
- Add cost tracking tags to all resources
- Set up budget alerts
- **Monitoring**: Databricks System Tables for cost analysis per table/job

#### 4. Access Control
**Current**: Assumes catalog/schema/volume exist
**Production**:
- Row-level security on Gold tables (filter by user department)
- Column masking for PII (if population data included SSN, etc.)
- Separate service principals for ingestion vs. analytics
- Audit logging for data access
- **Unity Catalog Features**: Dynamic views, attribute-based access control

#### 5. Monitoring & Alerting
**Current**: DLT event log only
**Production**:
- Data quality metrics dashboard (Grafana/Tableau)
- Freshness SLAs (alert if ingestion > 24 hours old)
- Row count anomaly detection (alert if daily change > 50%)
- Pipeline failure notifications (PagerDuty/Slack)
- End-to-end latency tracking

#### 6. Error Handling
**Current**: DLT expectations drop bad records
**Production**:
- Quarantine table for rejected records (don't drop silently)
- Configurable thresholds (fail if >5% dropped)
- Dead letter queue for failed API calls
- Retry logic with exponential backoff
- **Why**: Silent failures hide data quality issues

#### 7. Testing Strategy
**Current**: Manual validation
**Production**:
- Unit tests for ingestion logic (pytest)
- Integration tests for DLT pipeline (Databricks Asset Bundle test targets)
- Data quality tests (Great Expectations)
- CI/CD pipeline (GitHub Actions → DAB deploy)
- Staging environment before production promotion

## Retrospective: What Was Hardest

### 1. TSV Column Name Spaces
**The Problem**: BLS files have trailing spaces in headers (`series_id        \t`)
**Why Hard**: 
- Delta Lake rejected column names with spaces
- Needed to preserve raw data in Bronze (audit trail)
- Couldn't just fix at ingestion (would lose reproducibility)

**Solution Journey:**
1. First attempt: Strip at Bronze → Lost raw format
2. Second: Ignore the issue → Pipeline broke with DELTA_INVALID_CHARACTERS
3. Final: Delta column mapping at Bronze + clean at Silver

**Lesson**: Always preserve raw data as-is, clean in downstream layers

### 2. Streaming Deduplication
**The Problem**: Window functions only deduplicate within micro-batch
**Why Hard**: 
- Spark structured streaming processes in batches
- Each batch is independent
- Historical data not considered during row_number()

**Failed Attempt:**
```python
# This only deduplicates WITHIN each batch, not across table!
window_spec = Window.partitionBy("series_id", "year", "period")
df.withColumn("row_num", F.row_number().over(window_spec))
```

**Solution**: DLT's `apply_changes()` with SCD Type 1
- Maintains state across batches
- True upsert behavior (newer overwrites older)

### 3. Population JSON Format
**The Problem**: API returns nested JSON, need streaming-compatible format
**Why Hard**: 
- multiLine JSON doesn't work well with Auto Loader streaming
- NDJSON requires flattening API response during ingestion
- Wanted to preserve raw API response in Bronze

**Solution**: Save as single-line compact JSON
- Read as text in Bronze (one row = entire file)
- Parse with from_json() in Silver
- Explode array to create multiple rows

**Trade-off**: Small files (~20 records) this works, large files would need NDJSON

## Key Takeaways

1. **Preserve Raw Data**: Bronze should be append-only, exact copy of source
2. **Clean Systematically**: All cleaning in Silver, never in ad-hoc Gold queries
3. **DLT Patterns**: Use apply_changes() for streaming deduplication, not window functions
4. **Real-World Data is Messy**: TSV spaces, HTML parsing, missing columns - expect surprises
5. **Idempotency Matters**: Hash-based tracking saved hours of debugging re-runs

**Overall**: Good foundation for prototype/demo, needs operational tooling for enterprise production.
