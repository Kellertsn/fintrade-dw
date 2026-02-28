import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner":        "fintrade",
    "retries":      2,
    "retry_delay":  timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR      = "/opt/airflow/dbt/fintrade"
DBT_BIN      = os.getenv("DBT_BIN", "/home/airflow/.local/bin/dbt")
DBT_PROFILES = f"--profiles-dir {DBT_DIR}"

with DAG(
    dag_id="fintrade_pipeline",
    default_args=default_args,
    description=(
        "FinTrade daily pipeline: "
        "Alpha Vantage API → S3 (Parquet) → PostgreSQL → dbt staging → test → core → mart"
    ),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fintrade"],
) as dag:

    # ── Step 1: Fetch market data from Alpha Vantage,
    #           save raw JSON + Parquet to S3, load to raw.daily_prices
    fetch_and_load = BashOperator(
        task_id="fetch_and_load_prices",
        bash_command=(
            "cd /opt/airflow/data_generator && "
            "python generate_data.py"
        ),
    )

    # ── Step 2: dbt staging layer (views — clean & rename raw data)
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} run {DBT_PROFILES} --select staging"
        ),
    )

    # ── Step 3: Data quality tests on staging
    #           Fails the DAG early if raw data has issues (null PKs, bad values, etc.)
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} test {DBT_PROFILES} --select staging"
        ),
    )

    # ── Step 4: dbt core layer (star schema — dims + facts)
    dbt_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} run {DBT_PROFILES} --select core"
        ),
    )

    # ── Step 5: dbt mart layer (business-facing aggregate tables)
    dbt_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} run {DBT_PROFILES} --select mart"
        ),
    )

    # ── Step 6: Final data quality gate on mart tables
    dbt_test_mart = BashOperator(
        task_id="dbt_test_mart",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} test {DBT_PROFILES} --select mart"
        ),
    )

    # Pipeline DAG:
    # fetch → staging → test_staging → core → mart → test_mart
    fetch_and_load >> dbt_staging >> dbt_test_staging >> dbt_core >> dbt_mart >> dbt_test_mart
