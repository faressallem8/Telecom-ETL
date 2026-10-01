# 📡 Telecom Transactions ETL Pipeline

An end-to-end ETL project that ingests raw, messy telecom event files (pipe-delimited CSVs), cleans and validates them, enriches each record with a subscriber ID from a reference dimension, and loads the result into a **SQL Server** data warehouse. Rows that fail validation are not lost: they are routed to a dedicated error table for later review.

The project is built around a database named `SSIS_Telecom_DB` and uses **Python (pandas + pyodbc)** for the cleaning and loading logic.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Data Model](#data-model)
- [Source Data](#source-data)
- [Transformation Rules](#transformation-rules)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Known Limitations & Future Improvements](#known-limitations--future-improvements)
- [License](#license)

---

## Overview

Telecom networks generate a constant stream of events (a subscriber's device connecting to a cell tower, for example). Raw feeds are rarely clean: IMSIs go missing, cell and LAC values are blank, timestamps are malformed, and IDs contain stray characters.

This pipeline:

1. Reads batches of raw event files from a `Source Files` folder.
2. Separates incomplete records and writes them to an **error table**.
3. Standardises data types and formats on the valid records.
4. Looks up the `subscriber_id` for each IMSI from a **dimension table**.
5. Splits the IMEI into its **TAC** and **SNR** components.
6. Writes cleaned files to a `Processed Files` folder and bulk-loads them into the **`fact_transaction`** table.

## Architecture

```
 Source Files (pipe-delimited CSV)
            │
            ▼
   ┌─────────────────────┐
   │   cleaning.py       │
   │   (pandas)          │
   └─────────────────────┘
      │               │
      │ rows with     │ valid rows
      │ nulls         │
      ▼               ▼
 error_destination   enrich with dim_imsi_reference
 _output                 (left join on imsi)
                          │
                          ▼
                  split IMEI → TAC + SNR
                          │
                          ▼
                  Processed Files (CSV)
                          │
                          ▼
                  fact_transaction (SQL Server)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3 |
| Data processing | pandas |
| Database connectivity | pyodbc (ODBC Driver 17 for SQL Server) |
| Data warehouse | Microsoft SQL Server (Express) |

## Repository Structure

```
.
├── Create_database.sql        # Creates SSIS_Telecom_DB, fact_transaction and error_destination_output
├── Create_dim_imsi.sql        # Creates and populates dim_imsi_reference (IMSI → subscriber_id)
├── cleaning.py                # Main ETL script: clean, enrich, and load
├── Source Files/              # Raw input files
│   ├── 01_clean_data.csv
│   ├── 02_clean_data_with_null.csv
│   ├── 03_sample_data.csv
│   ├── batch_01_file_01.csv … batch_01_file_05.csv
│   └── batch_02_file_01.csv … batch_02_file_05.csv
└── Processed Files/           # Output of the cleaning step (created by the pipeline)
```

## Data Model

### `fact_transaction`
The cleaned, enriched event table.

| Column | Type | Description |
|--------|------|-------------|
| `id` | `int identity` | Surrogate primary key |
| `transaction_id` | `int` | Original event ID from the source file |
| `imsi` | `varchar(9)` | International Mobile Subscriber Identity |
| `subscriber_id` | `int` | Looked up from `dim_imsi_reference` (`-99999` if no match) |
| `tac` | `varchar(8)` | Type Allocation Code (first 8 digits of the IMEI) |
| `snr` | `varchar(6)` | Serial number (remaining IMEI digits) |
| `imei` | `varchar(14)` | Device identifier |
| `cell` | `int` | Cell tower ID |
| `lac` | `int` | Location Area Code |
| `event_type` | `varchar(1)` | Type of network event |
| `event_ts` | `datetime` | Event timestamp |

### `dim_imsi_reference`
Dimension table mapping each IMSI to a subscriber.

| Column | Type | Description |
|--------|------|-------------|
| `id` | `int identity` | Primary key |
| `imsi` | `varchar(9)` | IMSI |
| `subscriber_id` | `int` | Subscriber identifier |

### `error_destination_output`
Holds records rejected during validation (the structure mirrors SSIS-style error outputs, including `ErrorCode` and `ErrorColumn`).

## Source Data

Files are **pipe-delimited (`|`)** with the columns:

```
id | imsi | imei | cell | lac | event_type | event_ts
```

| File(s) | Purpose |
|---------|---------|
| `01_clean_data.csv` | Baseline sample of well-formed data |
| `02_clean_data_with_null.csv` | Data with missing values, to exercise the null-handling logic |
| `03_sample_data.csv` | Deliberately dirty data (e.g. non-numeric IDs such as `1@` and `text`, invalid event type `i`, missing fields) |
| `batch_01_file_01–05.csv`, `batch_02_file_01–05.csv` | Production-style batches processed by the pipeline |

> The pipeline skips the first three files in `Source Files` (the test files above) and processes only the batch files.

## Transformation Rules

| Step | Rule |
|------|------|
| **Null validation** | Rows with a null in `imsi`, `cell`, `lac`, `event_type` or `event_ts` are written to `error_destination_output` and excluded from the fact load. |
| **Type normalisation** | `imsi` and `event_type` are cleaned of decimal artifacts (e.g. `123.0` → `123`). `cell` and `lac` are cast to integers. |
| **Timestamp parsing** | `event_ts` is parsed with the format `%d/%m/%Y %H:%M`. |
| **Subscriber enrichment** | Left join to `dim_imsi_reference` on `imsi`. Unmatched IMSIs receive `subscriber_id = -99999`. |
| **IMEI split** | `TAC` = first 8 characters, `SNR` = remaining characters. Missing values default to `'-99999'`. |
| **Output** | A cleaned CSV per source file is written to `Processed Files/`, then bulk-inserted into `fact_transaction`. |

## Getting Started

### Prerequisites

- Python 3.8+
- Microsoft SQL Server (the script targets `localhost\SQLEXPRESS`)
- [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)
- Python packages:

```bash
pip install pandas pyodbc
```

### Database setup

Run the SQL scripts in SQL Server Management Studio (or `sqlcmd`) in this order:

1. `Create_database.sql`: creates the database, `fact_transaction` and `error_destination_output`
2. `Create_dim_imsi.sql`: creates and seeds `dim_imsi_reference`

### Folder setup

Create the two working folders next to `cleaning.py`:

```
Source Files/      ← place the raw CSVs here
Processed Files/   ← empty folder, filled by the pipeline
```

### Configuration

The connection string lives at the top of `cleaning.py`. Adjust it for your environment:

```python
conn = pyodbc.connect(
    "driver={ODBC Driver 17 for SQL Server};"
    "server=localhost\\SQLEXPRESS;"
    "database=SSIS_Telecom_DB;"
    "trusted_connection=yes;"
)
```

## Usage

```bash
python cleaning.py
```

The script prints progress for each file ("File N read", "nulls dropped of file N"). When it finishes, verify the load:

```sql
SELECT COUNT(*) FROM fact_transaction;
SELECT COUNT(*) FROM error_destination_output;

-- Records that could not be matched to a subscriber
SELECT COUNT(*) FROM fact_transaction WHERE subscriber_id = -99999;
```

## Known Limitations & Future Improvements

- **Error table insert**: `error_destination_output` has 11 columns but the insert statement supplies 7 values. Specify an explicit column list (and populate or default `ErrorCode` / `ErrorColumn`) so the rejected-row load is reliable.
- **Reload safety**: the load step inserts every file in `Processed Files/`, so re-running the script can create duplicates. Consider clearing the folder or adding an idempotent load (e.g. a staging table or a unique constraint).
- **Duplicate source IDs**: some batch files reuse the same `id` ranges (for example `batch_02_file_03` and `batch_02_file_04`). Consider a deduplication step or a composite key.
- **Hard-coded paths**: Windows-style paths (`Source Files\\`) and a fixed `[3:]` slice limit portability. Use `pathlib` and a file-name pattern such as `batch_*.csv`.
- **Invalid values**: non-numeric IDs and invalid event types (as in `03_sample_data.csv`) are only partly validated. Add stricter type checks that route failures to the error table.
- **Sleep delay**: the `time.sleep(2)` between files is not needed for correctness and could be removed or made configurable.
- **Planned**: logging, unit tests, and config via environment variables.

## License

Add a license of your choice (e.g. MIT) and update this section.

---

*Built as a hands-on data engineering project covering data cleaning, dimensional modelling, and SQL Server loading.*
