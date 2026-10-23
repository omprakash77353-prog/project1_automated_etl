"""
Project 1: Automated ETL Pipeline
Reads customers.csv, validates and cleans records, writes cleaned output.
Optional Snowflake loading can be enabled with environment variables.
"""
import os
import sys
import pandas as pd


REQUIRED_COLUMNS = {"customer_id", "name", "email"}


def extract(path: str) -> pd.DataFrame:
    """Read source CSV and verify required columns."""
    df = pd.read_csv(path)
    missing = REQUIRED_COLUMNS - set(df.columns.str.lower())
    if missing:
        raise ValueError(f"CSV is missing required columns: {sorted(missing)}")
    df.columns = [c.lower().strip() for c in df.columns]
    return df


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean records and separate invalid rows."""
    df = df.copy()
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce")
    df["name"] = df["name"].astype("string").str.strip()
    df["email"] = df["email"].astype("string").str.strip().str.lower()

    invalid_mask = (
        df["customer_id"].isna()
        | df["name"].isna()
        | df["name"].eq("")
        | df["email"].isna()
        | df["email"].eq("")
    )
    rejected = df[invalid_mask].copy()
    valid = df[~invalid_mask].drop_duplicates(subset=["customer_id"]).copy()
    valid["customer_id"] = valid["customer_id"].astype("int64")
    return valid, rejected


def load_csv(df: pd.DataFrame, output_path: str) -> None:
    """Load transformed rows to a new CSV output."""
    df.to_csv(output_path, index=False)


def load_to_snowflake(df: pd.DataFrame) -> None:
    """Optional Snowflake load; configure credentials as environment variables."""
    try:
        from snowflake.snowpark import Session
    except ImportError as exc:
        raise RuntimeError(
            "Snowpark is not installed. Run: pip install snowflake-snowpark-python"
        ) from exc

    required_env = [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE",
    ]
    missing = [key for key in required_env if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            "Missing Snowflake environment variables: " + ", ".join(missing)
        )

    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "role": os.environ["SNOWFLAKE_ROLE"],
    }
    session = Session.builder.configs(connection_parameters).create()
    try:
        session.write_pandas(
            df,
            table_name="CUSTOMERS",
            auto_create_table=True,
            overwrite=False,
        )
        print("Loaded records into Snowflake table CUSTOMERS.")
    finally:
        session.close()


def main() -> int:
    input_path = sys.argv[1] if len(sys.argv) > 1 else "customers.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "cleaned_customers.csv"
    try:
        raw = extract(input_path)
        valid, rejected = transform(raw)
        load_csv(valid, output_path)
        rejected.to_csv("rejected_customers.csv", index=False)
        print(f"Input records: {len(raw)}")
        print(f"Valid records: {len(valid)}")
        print(f"Rejected records: {len(rejected)}")
        print(f"Cleaned output: {output_path}")
        if os.getenv("LOAD_TO_SNOWFLAKE", "").lower() == "true":
            load_to_snowflake(valid)
        return 0
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"ETL failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
