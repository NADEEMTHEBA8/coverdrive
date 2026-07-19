"""Daily refresh DAG: Bronze ingestion → Silver transform → quality gate → dbt build."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

import pendulum
import requests
import structlog
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator

log = structlog.get_logger(__name__)

# ─── Configuration ───────────────────────────────────────────────────────────

LOCAL_TZ = pendulum.timezone("UTC")
DBT_PROJECT_DIR = "/opt/airflow/dbt"

# ─── Failure / SLA callbacks ─────────────────────────────────────────────────


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

        # Controlled by COVERDRIVE_INGEST_MODE; defaults to live scrape.
        mode = os.environ.get("COVERDRIVE_INGEST_MODE", "scrape")
        return run_ingestion(mode=mode)

    transform_silver = BashOperator(
        task_id="transform_silver",
        retries=0,
        bash_command="python -m coverdrive.processing.silver_pyspark_etl",
        env={
            "SILVER_S3_PATH": os.environ.get("SILVER_S3_PATH") or "s3a://coverdrive-lake/silver/",
            "GOLD_S3_PATH": os.environ.get("GOLD_S3_PATH") or "s3a://coverdrive-lake/gold/",
            "COVERDRIVE_S3_ENDPOINT": os.environ.get("COVERDRIVE_S3_ENDPOINT", ""),
            "COVERDRIVE_S3_ACCESS_KEY": os.environ.get("COVERDRIVE_S3_ACCESS_KEY", ""),
            "COVERDRIVE_S3_SECRET_KEY": os.environ.get("COVERDRIVE_S3_SECRET_KEY", ""),
        },
        append_env=True,
    )

    @task(task_id="quality_gate", retries=0)  # Quality failures shouldn't retry.
    def quality_gate() -> None:
        """Hard quality gate. Halts the DAG before dbt if Silver is bad."""
        from coverdrive.quality import run_quality_gate

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
            "AWS_REGION": os.environ.get("AWS_REGION", "ap-south-1"),
            "COVERDRIVE_LAKE_BUCKET": os.environ["COVERDRIVE_LAKE_BUCKET"],
        },
        append_env=True,
    )

    @task(task_id="warm_api_cache")
    def warm_api_cache() -> dict[str, Any]:
        """Touch the API readiness endpoint so the FastAPI process opens DuckDB."""
        try:
            response = requests.get("http://api:8000/readyz", timeout=10)
            return {"status_code": response.status_code, "body": response.json()}
        except requests.RequestException as e:
            # Non-blocking: API may not be deployed in every environment.
            log.warning("api.warmup.skipped", reason=str(e))
            return {"status": "skipped", "reason": str(e)}

    # ─── Wiring ──────────────────────────────────────────────────────────────
    bronze = ingest_bronze()
    bronze >> transform_silver >> quality_gate() >> dbt_build >> warm_api_cache()


coverdrive_daily_refresh()
