# Project 1 — Automated ETL Pipeline (Python + Snowflake)

## What it does
1. Extracts records from `customers.csv`.
2. Validates required columns and required values.
3. Cleans whitespace and lowercases email addresses.
4. Removes duplicate customer IDs.
5. Writes `cleaned_customers.csv` and `rejected_customers.csv`.
6. Optionally loads valid records into Snowflake table `CUSTOMERS`.

## Requirements
- Python 3.10+
- pandas
- Optional: a Snowflake account and `snowflake-snowpark-python`

## Run locally
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python etl_pipeline.py
```

You can specify input/output paths:
```bash
python etl_pipeline.py customers.csv cleaned_customers.csv
```

## Optional Snowflake load
Install `snowflake-snowpark-python`, set the variables shown in `.env.example` in your shell, then set:
```bash
LOAD_TO_SNOWFLAKE=true
python etl_pipeline.py
```
The code uses `Session.builder.configs()` and `session.write_pandas()`. Keep credentials out of source control.

## Outputs
- `cleaned_customers.csv`: valid, deduplicated rows
- `rejected_customers.csv`: rows rejected by validation

This is a starter portfolio project. Test it in your own environment before describing performance metrics on a resume.
