"""Core Telemetry Refresh DAG: Extraction -> Silver Processing -> Pandera Quality Gate -> dbt."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

# Try importing Astronomer Cosmos for granular dbt model DAG rendering
try:
    from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig
    from cosmos.providers.dbt.task_group import DbtTaskGroup

    HAS_COSMOS = True
except ImportError:
    HAS_COSMOS = False

LOCAL_TZ = pendulum.timezone("UTC")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")


def task_failure_callback(context: dict[str, Any]) -> None:
    """Log structured failure detail on task failure."""
    ti: TaskInstance = context["task_instance"]
    log.error(
        "task.failed",
        task_id=ti.task_id,
        dag_id=ti.dag_id,
        run_id=context.get("run_id"),
        try_number=ti.try_number,
        max_tries=ti.max_tries,
        exception=str(context.get("exception")),
    )


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:  # type: ignore[no-untyped-def]  # noqa: ANN001
    """Log SLA-miss warning."""
    sla_lines = ", ".join(f"{sla.task_id}" for sla in slas)
    log.warning("sla.missed", dag_id=dag.dag_id, tasks=sla_lines)


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=6),
    "on_failure_callback": task_failure_callback,
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="coverdrive_telemetry_refresh",
    description=(
        "Core telemetry refresh: ingest ESPN/Cricsheet/Weather → Silver → "
        "Pandera contracts → dbt Gold marts → API readiness check."
    ),
    schedule="0 0 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    tags=["coverdrive", "core-pipeline", "dbt"],
    doc_md=__doc__,
)
def coverdrive_telemetry_refresh() -> None:
    @task(task_id="extract_espn_telemetry")
    def extract_espn_telemetry() -> None:
        try:
            from src.ingestion.espn_html_extractor import run_ingestion
        except ImportError:
            from coverdrive.extract.espn_html_extractor import run_ingestion

        run_ingestion(mode="scrape")

    @task(task_id="extract_cricsheet_archive")
    def extract_cricsheet_archive() -> None:
        try:
            from src.ingestion.cricsheet_archive import extract_telemetry_archives
        except ImportError:
            from coverdrive.extract.cricsheet_archive import extract_telemetry_archives

        extract_telemetry_archives()

    @task(task_id="extract_weather_api")
    def extract_weather_api() -> None:
        try:
            from src.ingestion.open_meteo_api import run_ingestion
        except ImportError:
            from coverdrive.extract.open_meteo_api import run_ingestion

        run_ingestion()

    transform_silver = BashOperator(
        task_id="transform_silver",
        retries=0,
        bash_command="python -m src.ingestion.silver_pyspark_etl",
        env={
            "SILVER_S3_PATH": os.environ.get("SILVER_S3_PATH") or "s3a://coverdrive-lake/silver/",
            "GOLD_S3_PATH": os.environ.get("GOLD_S3_PATH") or "s3a://coverdrive-lake/gold/",
            "COVERDRIVE_S3_ENDPOINT": os.environ.get("COVERDRIVE_S3_ENDPOINT", ""),
            "COVERDRIVE_S3_ACCESS_KEY": os.environ.get("COVERDRIVE_S3_ACCESS_KEY", ""),
            "COVERDRIVE_S3_SECRET_KEY": os.environ.get("COVERDRIVE_S3_SECRET_KEY", ""),
        },
        append_env=True,
    )

    transform_cricsheet_silver = BashOperator(
        task_id="transform_cricsheet_silver",
        retries=0,
        bash_command="python -m src.ingestion.silver_cricsheet_etl",
        env={
            "COVERDRIVE_S3_ENDPOINT": os.environ.get("COVERDRIVE_S3_ENDPOINT", ""),
            "COVERDRIVE_S3_ACCESS_KEY": os.environ.get("COVERDRIVE_S3_ACCESS_KEY", ""),
            "COVERDRIVE_S3_SECRET_KEY": os.environ.get("COVERDRIVE_S3_SECRET_KEY", ""),
        },
        append_env=True,
    )

    transform_weather_silver = BashOperator(
        task_id="transform_weather_silver",
        retries=0,
        bash_command="python -m src.ingestion.silver_weather_etl",
        env={
            "COVERDRIVE_S3_ENDPOINT": os.environ.get("COVERDRIVE_S3_ENDPOINT", ""),
            "COVERDRIVE_S3_ACCESS_KEY": os.environ.get("COVERDRIVE_S3_ACCESS_KEY", ""),
            "COVERDRIVE_S3_SECRET_KEY": os.environ.get("COVERDRIVE_S3_SECRET_KEY", ""),
        },
        append_env=True,
    )

    @task(task_id="enforce_silver_data_contracts", retries=0)
    def enforce_silver_data_contracts() -> None:
        """Hard quality gate. Halts DAG before dbt execution if Pandera contracts fail."""
        try:
            from src.quality.validation_rules import run_quality_gate
        except ImportError:
            from coverdrive.contracts.pandera_gates import run_quality_gate

        run_quality_gate()

    if HAS_COSMOS:
        transform_gold_marts = DbtTaskGroup(
            group_id="transform_gold_marts",
            project_config=ProjectConfig(DBT_PROJECT_DIR),
            profile_config=ProfileConfig(
                profile_name="coverdrive",
                target_name="dev",
            ),
            execution_config=ExecutionConfig(
                dbt_executable_path="/usr/local/bin/dbt",
            ),
        )
    else:
        transform_gold_marts = BashOperator(
            task_id="dbt_build",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "dbt deps --no-version-check && "
                "dbt build --target=dev --fail-fast"
            ),
            env={
                "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
                "AWS_REGION": os.environ.get("AWS_REGION", "ap-south-1"),
                "COVERDRIVE_LAKE_BUCKET": os.environ.get("COVERDRIVE_LAKE_BUCKET", "coverdrive"),
            },
            append_env=True,
        )

    @task(task_id="warm_api_cache")
    def warm_api_cache() -> dict[str, Any]:
        """Touch API readiness endpoint so FastAPI opens DuckDB."""
        try:
            response = requests.get("http://api:8000/readyz", timeout=10)
            return {"status_code": response.status_code, "body": response.json()}
        except requests.RequestException as e:
            log.warning("api.warmup.skipped", reason=str(e))
            return {"status": "skipped", "reason": str(e)}

    # ─── DAG Lineage ────────────────────────────────────────────────────────
    espn_task = extract_espn_telemetry()
    cricsheet_task = extract_cricsheet_archive()
    weather_task = extract_weather_api()

    silver_espn = espn_task >> transform_silver
    silver_cricsheet = cricsheet_task >> transform_cricsheet_silver
    silver_weather = weather_task >> transform_weather_silver

    q_gate = enforce_silver_data_contracts()

    (
        [silver_espn, silver_cricsheet, silver_weather]
        >> q_gate
        >> transform_gold_marts
        >> warm_api_cache()
    )


coverdrive_telemetry_refresh()
