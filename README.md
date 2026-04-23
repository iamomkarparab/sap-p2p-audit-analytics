SAP P2P Audit Analytics — End-to-End Azure Data Engineering Pipeline
![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-blue)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Optimized-green)
![ADF](https://img.shields.io/badge/ADF-Orchestration-lightblue)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)
---
## Project Overview

An end-to-end Azure Data Engineering pipeline that ingests SAP 
Procure-to-Pay (P2P) data from on-premises CSV files, processes 
it through Bronze → Silver → Gold layers, and generates analytics 
for strengthening internal controls of the organisation.

This project automates and scales P2P data processing on Azure, 
enabling procurement teams to detect anomalies, enforce compliance, 
and improve financial governance across the organisation.
---
Architecture
```
On-Premises SAP CSV Files
        │
        ▼
┌─────────────────────────────────────────────────────┐
│           BRONZE LAYER (Azure Data Factory)          │
│                                                      │
│  SHIR → ADF Pipeline → ADLS Bronze Container        │
│  • Incremental load (file LastWriteTime watermark)  │
│  • Schema validation (143 ekko / 307 ekpo columns)  │
│  • Full audit trail (SQL file_tracking table)        │
│  • Idempotency (no duplicate processing)             │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│          SILVER LAYER (Azure Databricks)             │
│                                                      │
│  Bronze CSVs → PySpark Notebook → po_report Delta   │
│  • Incremental load (recordstamp watermark)          │
│  • Deduplication (latest record per PO)              │
│  • Broadcast join (ekko + ekpo → ME2N columns)       │
│  • MERGE into Delta (SCD Type 1)                     │
│  • OPTIMIZE + Z-Order + VACUUM                       │
│  • Partitioned by po_year + po_month                 │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│           GOLD LAYER (Azure Databricks)              │
│                                                      │
│  po_report → po_filtered → ptp_test_1 Delta          │
│  • P2P filters (no STO, no deleted, price > 0)       │
│  • PO_KEY generation (material+month+year+plant)     │
│  • Price variance detection (>= 5% threshold)        │
│  • CASE_ID assignment (dense_rank)                   │
│  • 2-month reprocessing window for accuracy          │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│              POWER BI DASHBOARD                      │
│                                                      │
│  • Price Variance Audit Report                       │
│  • KPI Cards + Bar Charts + Detail Table             │
│  • Connected to Gold Delta via ADLS Gen2             │
└─────────────────────────────────────────────────────┘
```
---
Tech Stack
Component	Technology
Orchestration	Azure Data Factory (ADF)
Processing	Azure Databricks (PySpark)
Storage	Azure Data Lake Storage Gen2
File Format	Delta Lake (Parquet + Transaction Log)
Source System	SAP ECC (ekko + ekpo tables)
SQL Layer	Azure SQL Database (Free Tier)
Security	Azure Key Vault
Version Control	GitHub (ADF Git Integration)
Visualization	Power BI Desktop
Runtime	Self-Hosted Integration Runtime (SHIR)
---
Project Structure
```
sap-p2p-audit-analytics/
├── adf/                          ← ADF pipeline JSONs (auto-synced)
│   ├── pipeline/
│   │   ├── pl_master_dev.json
│   │   ├── pl_bronze_ingest_dev.json
│   │   ├── pl_bronze_copy_dev.json
│   │   ├── pl_silver_dev.json
│   │   └── pl_gold_dev.json
│   ├── linkedService/
│   ├── dataset/
│   └── trigger/
├── notebooks/                    ← Databricks PySpark notebooks
│   ├── nb_silver_po_report_dev.py
│   ├── nb_gold_po_filtered_dev.py
│   └── nb_gold_ptp_test_1_dev.py
├── sql/                          ← Azure SQL scripts
│   └── bronze_sql_v3_fresh.sql
├── screenshots/                  ← Project screenshots
└── README.md
```
---
Pipeline Details
Bronze Layer (ADF)
Pipeline: `pl_master_dev` → `pl_bronze_ingest_dev` → `pl_bronze_copy_dev`
Key features:
Self-Hosted Integration Runtime (SHIR) for on-premises file access
Watermark-based incremental load using file LastWriteTime
Schema validation — ekko (143 columns), ekpo (307 columns)
Sequential file processing to prevent partial loads
Full audit trail in Azure SQL:
`dbo.watermark` — tracks last processed file timestamp
`dbo.file_tracking` — records every file processed
`dbo.pipeline_run_audit` — logs pipeline execution details
`dbo.table_config` — stores table configuration
6 stored procedures for watermark management and audit logging
Idempotency check — prevents duplicate file processing
Schedule: Daily at 1:00 AM IST via `tr_daily_master` trigger
---
Silver Layer (Databricks)
Notebook: `nb_silver_po_report_dev`
Key features:
Incremental load using recordstamp-based watermark
Reads ekko + ekpo CSVs from Bronze layer
Selects ME2N report columns only
Deduplication — keeps latest record per PO/line item
Broadcast join — ekko (header) joined with ekpo (line items)
MERGE into `po_report` Delta table (SCD Type 1)
Delta Lake optimizations:
Auto Optimize + Auto Compact
OPTIMIZE + Z-Order (vendor_id, material_number, po_number)
VACUUM (7-day retention)
Schema Evolution enabled
Partitioned by `po_year` + `po_month`
Output: `silver/po_report/` (32 columns, Delta format)
---
Gold Layer (Databricks)
Notebook 1: `nb_gold_po_filtered_dev`
Key features:
Reads po_report from Silver (incremental)
Applies P2P audit filters:
Removes STO (Stock Transfer Orders)
Removes deleted line items
Removes null/empty materials
Removes zero price and zero quantity
Generates PO_KEY = material + month + year + plant + description
MERGE into `po_filtered` Delta table
Reusable foundation for all 45 P2P audit use cases
Output: `gold/po_filtered/` (Delta format)
---
Notebook 2: `nb_gold_ptp_test_1_dev` (Use Case 1 — Price Variance)
Key features:
Reads po_filtered with 2-month reprocessing window
Groups by PO_KEY → detects multiple prices per material
Calculates price_diff and price_variance
Filters cases with max_variance >= 5%
Assigns CASE_ID using dense_rank
MERGE into `ptp_test_1` Delta table
Month-based watermark for accurate reprocessing
Output: `gold/ptp_test_1/` (Delta format)
---
Audit Use Case — Price Variance (PTP_TEST_1)
Business Problem:
Same material purchased at different prices within same month → potential procurement fraud or pricing irregularity.
Detection Logic:
```
1. Group POs by PO_KEY (material + month + year + plant)
2. Count distinct prices per PO_KEY
3. If count > 1 → price variance exists
4. Calculate: price_variance = (net_price - min_price) / min_price
5. Flag cases where max_variance >= 5%
6. Assign CASE_ID for audit tracking
```
Sample Output:
```
case_id | po_number  | material | net_price | min_price | variance
1       | 4500000001 | C0000003 | 10.0      | 5.0       | 100%  ← FLAGGED
1       | 4500000000 | C0000003 | 5.0       | 5.0       | 0%    ← BASELINE
```
---
Azure Resources
Resource	Name	Purpose
Resource Group	`rg-p2p-dev-cin`	Container for all resources
ADF	`adf-onprem-adls-ingestion-dev`	Pipeline orchestration
ADLS	`stsapadlsrawdev`	Data storage (bronze/silver/gold)
Databricks	`adb-sap-p2p-dev`	PySpark processing
SQL Database	`sqldb-p2p-free-dev`	Audit trail and watermark
Key Vault	`kv-sap-p2p-dev`	Secret management
SHIR	`IR-SHIR-ONPREM-ADLSDEV`	On-premises connectivity
---
Setup Instructions
Prerequisites
Azure subscription
Azure Data Factory
Azure Databricks workspace
Azure Data Lake Storage Gen2
Azure SQL Database (Free tier)
Azure Key Vault
Self-Hosted Integration Runtime installed on local machine
Power BI Desktop
Step 1 — SQL Setup
```sql
-- Run sql/bronze_sql_v3_fresh.sql in Azure SQL Query Editor
-- Creates all tables, indexes, stored procedures
```
Step 2 — ADLS Setup
```
Create 3 containers in ADLS:
→ bronze
→ silver
→ gold
```
Step 3 — Key Vault Setup
```
Create secret: adls-storage-key
Create Databricks secret scope: kv-sap-p2p-dev
Assign Key Vault Secrets User role to AzureDatabricks
```
Step 4 — ADF Setup
```
Import pipeline JSONs from adf/ folder
Configure linked services:
→ ls_filesystem_src_dev (SHIR)
→ ls_adls_bronze_dev (ADLS)
→ ls_sqldb_watermark_dev (SQL)
→ ls_databricks_dev (Databricks)
```
Step 5 — Databricks Setup
```
Upload notebooks from notebooks/ folder
Update storage_account_key in Cell 1 of each notebook
(or use Key Vault with dbutils.secrets.get)
```
Step 6 — Run Pipeline
```
Go to ADF → pl_master_dev → Debug
Monitor: Bronze → Silver → Gold
Verify output in ADLS containers
```
---
Data Flow
```
Source Files (on-premises):
→ ekko_YYYY-MM-DD.csv (PO Header — 143 columns)
→ ekpo_YYYY-MM-DD.csv (PO Line Items — 307 columns)

Bronze:
→ bronze/ekko/year=YYYY/month=MM/ekko_YYYY-MM-DD.csv
→ bronze/ekpo/year=YYYY/month=MM/ekpo_YYYY-MM-DD.csv

Silver:
→ silver/po_report/ (Delta, partitioned by po_year/po_month)
→ silver/po_report_watermark/ (Delta)

Gold:
→ gold/po_filtered/ (Delta, partitioned by po_year/po_month)
→ gold/po_filtered_watermark/ (Delta)
→ gold/ptp_test_1/ (Delta, partitioned by po_year/po_month)
→ gold/ptp_test_1_watermark/ (Delta)
```
---
Key Design Decisions
Why Medallion Architecture?
Separates raw, cleansed and business-ready data. Each layer serves different consumers and can be reprocessed independently.
Why Delta Lake?
ACID transactions, time travel, schema evolution, and MERGE operations make it ideal for incremental audit analytics.
Why SCD Type 1 for po_report?
POs are transactional — only current state matters. SCD Type 2 will be added when vendor master (lfa1) is integrated.
Why separate po_filtered notebook?
Reusable foundation for all 45 P2P audit use cases. Filters applied once, shared across all use cases.
Why month-based watermark for ptp_test_1?
Price variance analysis requires complete month data. 2-month reprocessing window handles late-arriving POs.
Why partitioning by po_year + po_month?
Gold layer queries are always month-specific. Partition pruning reduces scan from full table to single month folder.
---
Future Enhancements
SCD Type 2 for vendor master (lfa1) table
Azure Purview for data catalogue and lineage
CI/CD with Azure DevOps (ARM templates + Databricks CLI)
Full RBAC with Managed Identity (remove hardcoded credentials)
Additional P2P audit use cases (duplicate POs, split POs, approval bypass)
Real-time alerting via Microsoft Teams webhook
Row Level Security in Power BI
---
Author
Omkar Parab
Data Engineer | Mumbai, India
SAP P2P Audit Analytics | Azure Data Engineering
---
License
MIT License — feel free to use this project as a reference for your own data engineering portfolio.
