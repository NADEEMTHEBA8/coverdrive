# Screenshot Checklist — Coverdrive Visual Proof

This document is a manual action plan. Complete the steps in order; each screenshot depends on the pipeline stage before it being in a stable, passing state. Run `make demo` first and wait for it to exit cleanly before opening any UI.

---

## Screenshot 1 of 4 — Airflow DAG Success

**Target interface.** Airflow web UI at `http://localhost:8180`. Log in with `admin` / `admin`. Navigate to **DAGs → coverdrive_daily_refresh → Graph**. Select the most recent dag run from the **Runs** dropdown at the top of the graph view.

**Exact system state.** The DAG run must have completed — all five task nodes must show a dark green border (status = `success`). Do not capture while any task is in `running` (light green) or `queued` (grey) state. The correct run to capture is the one triggered by `make demo`, which executes the pipeline against the bundled fixtures. The run ID will start with `manual__` or `scheduled__2024`. Confirm by hovering over the `warm_api_cache` node — its tooltip must read `State: success`.

**Verification marker.** All five task boxes must be solid green in left-to-right order: `ingest_bronze → transform_silver → quality_gate → dbt_build → warm_api_cache`. The run duration shown in the top bar should be under 10 minutes. There must be no red or orange nodes anywhere in the graph. The DAG ID `coverdrive_daily_refresh` must be legible in the page header.

**Asset path.** Save as `docs/assets/airflow_dag_success.png`.

---

## Screenshot 2 of 4 — MinIO Bronze and Silver Partitions

**Target interface.** MinIO web console at `http://localhost:9101`. Log in with `minioadmin` / `minioadmin`. Navigate to **Object Browser → coverdrive bucket**. Expand the `bronze/` prefix, then expand `batting/`, and continue expanding until the Hive partition directory `ingestion_date=YYYY-MM-DD/` is visible with `data.parquet` inside it. Then collapse `bronze/` and expand `silver/` to the same depth so both partition trees are visible simultaneously in the same screenshot.

**Exact system state.** Capture this after `make demo` exits cleanly and after `make transform` (or the equivalent `transform_silver` Airflow task) has completed. Both `bronze/batting/ingestion_date=.../data.parquet` and `silver/batting/ingestion_date=.../data.parquet` must be populated. If you ran in fixtures mode (the default), the date suffix will be today's UTC date. Expand both `batting` and `bowling` sub-prefixes if the MinIO UI allows both to be visible in frame; if not, prioritise showing one layer of the Bronze prefix alongside one layer of the Silver prefix.

**Verification marker.** The left-side prefix tree must show at minimum: `bronze/`, `bronze/batting/`, and `silver/`, `silver/batting/`. The file `data.parquet` must be visible with a non-zero file size in the object list on the right. The bucket name `coverdrive` must appear in the breadcrumb at the top. A size value in kilobytes or megabytes next to `data.parquet` is the primary signal that real data was written and is not an empty file.

**Asset path.** Save as `docs/assets/minio_partitions.png`.

---

## Screenshot 3 of 4 — FastAPI Rankings Endpoint Response

**Target interface.** Local terminal — not a browser. Open a new terminal tab after `make demo` has finished and the API container is healthy. Run the following command exactly:

```bash
curl -s 'http://localhost:8000/api/v1/rankings/batsmen?limit=10' | python3 -m json.tool
```

Capture the terminal window showing the full JSON response. Do not use the Swagger UI for this screenshot — the raw JSON in a dark terminal is a cleaner proof of a working endpoint than a browser form.

**Exact system state.** The `dbt_build` Airflow task (or `make dbt-build`) must have completed successfully before hitting this endpoint. The API starts in a degraded state (`/readyz` returns non-OK) until the marts are populated. Confirm readiness first with `curl -s http://localhost:8000/readyz` — it must return `{"status": "ok"}` before capturing the rankings response. If it returns anything else, `make dbt-build` has not completed.

**Verification marker.** The JSON response must be an array of objects. Each object must contain at least a player name field and a `pca_score` numeric field. The list must contain exactly 10 entries (matching the `?limit=10` parameter). Player names recognisable as real ODI cricketers must appear — for example, `"sr tendulkar"` or `"rt ponting"` — confirming the fixtures data flowed through Bronze → Silver → the `mart_top_batsmen` dbt model correctly. The terminal prompt must show the command that was run above the output so the endpoint path is legible in the screenshot.

**Asset path.** Save as `docs/assets/api_batsmen_response.png`.

---

## Screenshot 4 of 4 — Pandera Quality Gate Terminal Output

**Target interface.** Local terminal, running `make quality` directly from the project root (not via Airflow — the Airflow log viewer truncates structured output). The command calls `python -m coverdrive.quality`, which writes structured logs via `structlog` in key=value format (dev mode) or JSON (if `COVERDRIVE_ENV=production` is set). Leave the default dev format so the output is human-readable in the screenshot.

**Exact system state.** Run this after `make transform` has populated the Silver partitions but before running `make dbt-build`. The quality gate must run against real Silver Parquet data on MinIO (or the local equivalent in fixtures mode). The command must exit with code `0`. Capture the terminal immediately after the command returns so the shell prompt showing `$` or `%` is visible below the last log line, confirming the process did not hang.

**Verification marker.** The terminal output must show two distinct `quality.passed` log lines — one for `table=batting` and one for `table=bowling` — each with a `rows=` value greater than zero. The final line must be the `quality.gate.passed_all` event with `tables=['batting', 'bowling']`. The exact log strings to look for are:

```
table=batting ... quality.passed ... rows=NNN
table=bowling ... quality.passed ... rows=NNN
quality.gate.passed_all tables=['batting', 'bowling']
```

No `quality.gate.failure` or `QualityGateFailure` text must appear anywhere in the visible output. The `rows=` count should be at least `20` for the fixtures path and at least `200` for a live scrape run.

**Asset path.** Save as `docs/assets/quality_gate_pass.png`.

---

*After all four screenshots are captured, create the directory if it does not exist (`mkdir -p docs/assets`) and confirm file names match the paths above exactly before pushing. The README image references are case-sensitive.*
