# deloitte_gfs_snowflake_pipeline
Enterprise GFS data validation in Snowflake: 35 duplicates detected, 12 outliers identified, 3 verified records
# Deloitte GFS Data Management Pipeline – Snowflake

## Executive Overview

This project simulates an enterprise-grade **Master Data Management (MDM) validation pipeline** built in Snowflake, aligned with Deloitte Global Financial Services (GFS) Data Management responsibilities.

The solution ingests multi-source company master data, applies structured data quality controls, and produces a governed, analytics-ready dataset using enterprise SQL patterns.

---

## What I Built

Designed and implemented a structured **RAW → VALIDATED ETL workflow** in Snowflake that:

- Profiled multi-source master data
- Detected duplicate entities across vendors
- Identified statistical outliers using Z-score validation
- Applied status-based governance classification
- Produced a clean, BI-ready validated dataset

All logic was built using modular SQL (CTEs, aggregations, window functions) following enterprise data warehouse standards.

---

## Architecture
RAW Schema (Unvalidated Multi-Source Data)
↓
Data Profiling & Quality Monitoring
↓
Duplicate Detection (Entity Resolution)
↓
Outlier Detection (Z-Score Statistical Analysis)
↓
Status Classification Logic
↓
VALIDATED Schema (Governed Output Table)


---

## Processing Logic

### 1. Duplicate Detection

```sql
SELECT company_name, COUNT(*)
FROM raw.company_data
GROUP BY company_name
HAVING COUNT(*) > 1;

2. Outlier Detection (Z-Score)

Formula applied:
(value - mean) / standard_deviation       Threshold used: ±1.5σ

3. Final Classification Logic

Records classified into:

a)VERIFIED

b)DUPLICATE_DETECTED

c)OUTLIER_DETECTED



Results
| Metric                       | Outcome  |
| ---------------------------- | -------- |
| Total Records Processed      | 50       |
| Duplicate Records Identified | 35 (70%) |
| Statistical Outliers Flagged | 12 (24%) |
| Verified Clean Records       | 3 (6%)   |


Sample Observations

a)High-revenue entities triggered positive Z-score alerts

b)Early-stage companies triggered negative deviation alerts

c)Multi-source naming variations created duplicate clusters



Business Impact

This pipeline demonstrates:

Structured data quality monitoring

Risk-based anomaly detection

Cross-source reconciliation capability

Governance-first ETL design

Production-scalable Snowflake implementation

The framework reduces reporting risk, improves master data reliability, and strengthens downstream financial analytics and compliance reporting readiness.



Technology Stack

Snowflake (Enterprise Cloud Data Warehouse)

ANSI SQL (CTEs, aggregations, window functions)

Z-score statistical validation

Multi-source entity reconciliation

RAW → VALIDATED governance workflow



Scalability Considerations

While demonstrated on 50 records, the architecture supports:

10M+ records via Snowflake warehouse scaling

Scheduled execution using Snowflake Tasks

dbt orchestration for production environments

Incremental validation workflows



Deloitte GFS Alignment

This project reflects core Associate Analyst responsibilities:

Data profiling and monitoring

Data quality issue identification

External vendor reconciliation

Enterprise SQL development

Governance-controlled transformation workflows



Repository Structure
deloitte-gfs-snowflake-pipeline/
│
├── gfs_pipeline.sql
├── gfs_validation_results.csv
├── gfs_duplicates.csv
├── gfs_outliers.csv
├── screenshots/
└── README.md


