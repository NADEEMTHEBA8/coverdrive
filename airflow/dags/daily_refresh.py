"""Daily refresh DAG: Bronze ingestion → Silver transform → quality gate → dbt build."""

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

# ─── Configuration ───────────────────────────────────────────────────────────

LOCAL_TZ = pendulum.timezone("UTC")
DBT_PROJECT_DIR = "/opt/airflow/dbt"

log = logging.getLogger(__name__)

# ─── Failure / SLA callbacks ─────────────────────────────────────────────────


def task_failure_callback(context: dict[str, Any]) -> None:
    """Log structured failure detail on task failure."""
    ti: TaskInstance = context["task_instance"]
    log.error(
        f"Task {ti.task_id} failed in DAG {ti.dag_id}. "
        f"Run: {context.get('run_id')}, Try: {ti.try_number}/{ti.max_tries}. "
        f"Exception: {context.get('exception')}"
    )


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:  # type: ignore[no-untyped-def]  # noqa: ANN001
    """Log SLA-miss warning."""
    sla_lines = ", ".join(f"{sla.task_id}" for sla in slas)
    log.warning(f"SLA missed on DAG {dag.dag_id} for tasks: {sla_lines}")


# ─── DAG definition ──────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=6),  # End-to-end completion budget.
    "on_failure_callback": task_failure_callback,
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="coverdrive_daily_refresh",
    description=(
        "Daily refresh: scrape ESPNcricinfo → Silver → quality gate → "
        "dbt build → API readiness check."
    ),
    schedule="0 0 * * *",  # 00:00 UTC daily
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,  # Don't backfill; historical scrapes hit the same source rows.
    max_active_runs=1,  # Avoid two scrapes overlapping at the source.
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    tags=["coverdrive", "batch", "production"],
    doc_md=__doc__,
)
def coverdrive_daily_refresh() -> None:
    @task(task_id="ingest_bronze")
    def ingest_bronze() -> dict[str, str]:
        """Scrape ESPNcricinfo → Bronze partitioned Parquet."""
        from coverdrive.ingestion import run_ingestion

        # `mode=scrape` for prod, `fixtures` for local dev / smoke tests.
        mode = os.environ.get("COVERDRIVE_INGEST_MODE", "scrape")
        return run_ingestion(mode=mode)

    @task(task_id="transform_silver")
    def transform_silver(bronze_uris: dict[str, str]) -> dict[str, str]:
        """Bronze → Silver: dedupe, type-cast, conform schema."""
        # bronze_uris is XCom-passed for lineage visibility in the Airflow UI.
        from coverdrive.transform import run_transform

        _ = bronze_uris  # Reference for XCom dependency
        return run_transform()

    @task(task_id="quality_gate", retries=0)  # Quality failures shouldn't retry.
    def quality_gate(silver_uris: dict[str, str]) -> None:
        """Hard quality gate. Halts the DAG before dbt if Silver is bad."""
        from coverdrive.quality import run_quality_gate

        _ = silver_uris
        run_quality_gate()

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt deps --no-version-check && "
            "dbt build --target=dev --fail-fast"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            "COVERDRIVE_S3_BUCKET": os.environ.get("COVERDRIVE_S3_BUCKET", "coverdrive"),
            "COVERDRIVE_S3_ACCESS_KEY": os.environ.get("COVERDRIVE_S3_ACCESS_KEY", "minioadmin"),
            "COVERDRIVE_S3_SECRET_KEY": os.environ.get("COVERDRIVE_S3_SECRET_KEY", "minioadmin"),
            "COVERDRIVE_S3_ENDPOINT_HOST": "minio:9000",
            "COVERDRIVE_WAREHOUSE_PATH": "/opt/airflow/data/warehouse.duckdb",
        },
        append_env=True,
    )

    @task(task_id="warm_api_cache")
    def warm_api_cache() -> dict[str, Any]:
        """Touch the API readiness endpoint so the FastAPI process opens DuckDB."""
        # In production this would also call cache-warmup endpoints for the most
        # popular queries (top 25 batsmen/bowlers).
        try:
            response = requests.get("http://api:8000/readyz", timeout=10)
            return {"status_code": response.status_code, "body": response.json()}
        except requests.RequestException as e:
            # Non-blocking: API may not be deployed in every environment.
            return {"status": "skipped", "reason": str(e)}

    # ─── Wiring ──────────────────────────────────────────────────────────────
    bronze = ingest_bronze()
    silver = transform_silver(bronze)
    quality_gate(silver) >> dbt_build >> warm_api_cache()


coverdrive_daily_refresh()
