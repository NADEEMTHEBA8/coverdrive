# Coverdrive

> A production-grade data platform built on a flawed MSc dissertation.
> The README is honest about both halves.

[![CI](https://github.com/nadeem/coverdrive/actions/workflows/ci.yml/badge.svg)](.github/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11-blue)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![Airflow](https://img.shields.io/badge/airflow-2.9-017CEE)
![DuckDB](https://img.shields.io/badge/duckdb-0.10-FFF000)
![License](https://img.shields.io/badge/license-MIT-green)

---

## What this is

Coverdrive ingests ~5,000 cricketers' career statistics from
ESPNcricinfo, lands them in a partitioned Parquet lakehouse, transforms
them in DuckDB with dbt, quality-gates the result with Pandera, serves
the marts via FastAPI, and orchestrates the whole thing with Airflow.
The entire stack — MinIO, Airflow, the warehouse, the API — runs
locally with **one command**:

```bash
make demo
```

It is also the **successor and corrective** to my 2022 MSc dissertation,
*"Predicting Greatest Cricketer by Comparing Different Machine Learning
Approaches."* That project reported 99% accuracy and was wrong. The
postmortem on what was wrong, and what now lives in this repo instead,
is in [`docs/adr-pca-leakage.md`](docs/adr-pca-leakage.md). If you read
nothing else in this repo, read that.

---

## Architecture at a glance

```
ESPNcricinfo
    │
    ▼ requests + tenacity retries
┌─────────────────────────────────────────────────────────────┐
│ S3 / MinIO lakehouse                                         │
│                                                              │
│   bronze/{table}/ingestion_date=YYYY-MM-DD/data.parquet      │
│       │   raw scrape, partitioned, idempotent                │
│       ▼   transform.py (pure functions)                      │
│   silver/{table}/ingestion_date=YYYY-MM-DD/data.parquet      │
│       │   typed, cleaned, deduped                            │
│       ▼   Pandera schema gate ── fail-fast on bad data       │
└───────┼──────────────────────────────────────────────────────┘
        │
        ▼  dbt-duckdb
┌─────────────────────────────────────────────────────────────┐
│ DuckDB warehouse                                             │
│                                                              │
│   staging/  →  marts/                                        │
│       dim_player · fact_career_stats                         │
│       mart_top_batsmen · mart_top_bowlers                    │
│                                                              │
│   compute_pca.sql macro ── PCA composite as a *metric*,      │
│                            not a learning target             │
└───────┼──────────────────────────────────────────────────────┘
        │
        ▼
   FastAPI    →    /api/v1/rankings/batsmen
                   /api/v1/rankings/bowlers
                   /api/v1/players/{name}/stats

   Orchestrated by Airflow (daily_refresh DAG · retries · SLA · Slack alerts)
   Infrastructure-as-code: Terraform module for AWS (S3 · RDS · ECR · IAM · CloudWatch)
```

Full diagram and component contracts in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Quickstart

### Prerequisites

- Docker + Docker Compose
- Python 3.11
- `make`
- ~2 GB free RAM

### One-command demo

```bash
git clone https://github.com/nadeem/coverdrive.git
cd coverdrive
cp .env.example .env
make demo
```

`make demo` brings up MinIO + Airflow, seeds Bronze from the test
fixtures (no live scrape), runs the transform, the quality gate, and the
dbt build, then starts the FastAPI server.

When it finishes you'll have:

| What | Where |
|---|---|
| Airflow UI | http://localhost:8080 (admin / admin) |
| MinIO console | http://localhost:9001 (minioadmin / minioadmin) |
| FastAPI docs | http://localhost:8000/docs |
| Warehouse | `data/warehouse.duckdb` (queryable with `duckdb` CLI) |

### Try the API

```bash
# Top 10 ODI batsmen by PCA composite (min 20 matches)
curl 'http://localhost:8000/api/v1/rankings/batsmen?limit=10' | jq

# Sachin Tendulkar's career stats
curl 'http://localhost:8000/api/v1/players/SR%20Tendulkar/stats' | jq
```

### Run against the live ESPN source

Fixtures mode is the default to keep CI deterministic. To hit the real
source:

```bash
unset COVERDRIVE_USE_FIXTURES
make ingest      # scrapes ESPNcricinfo with retries + backoff
make transform   # Bronze → Silver
make quality     # Pandera gate
make dbt-build   # warehouse
make api         # serve
```

---

## What's in the box

| Capability | Where to look |
|---|---|
| **Ingestion** with retries, backoff, and fixtures mode | `src/coverdrive/ingestion.py` |
| **Pure-function transforms** (testable from CSV alone) | `src/coverdrive/transform.py` |
| **Pandera schema gates** (halts on schema/range/null violations) | `src/coverdrive/quality.py` |
| **Medallion lakehouse** (Bronze/Silver/Gold) with Hive partitioning | `conf/pipeline.yaml`, `src/coverdrive/utils.py::build_partition_path` |
| **dbt project** with sources, staging, marts, tests, and a PCA macro | `dbt/` |
| **FastAPI serving layer** with `/healthz`, `/readyz`, request-logging middleware | `src/coverdrive/api.py` |
| **Airflow DAG** with retries, SLA, timeout, and Slack failure callback | `airflow/dags/daily_refresh.py` |
| **Terraform AWS module** — VPC, S3, RDS, ECR, IAM (least-priv), CloudWatch | `infra/terraform/` |
| **CI** — ruff format + lint, mypy, pytest with coverage, dbt parse | `.github/workflows/ci.yml` |
| **80%+ test coverage** — pytest + moto for S3 + on-disk fixtures | `tests/` |
| **Architecture Decision Record** for the PCA target-leakage fix | `docs/adr-pca-leakage.md` |

---

## Engineering decisions worth highlighting

A few choices I'd want a reviewer to ask about:

1. **The pipeline halts on schema violation; it does not retry.**
   `quality_gate` has `retries=0`. A bad-data failure is not a flake.
   It is a signal that the source changed, and re-running won't fix it.

2. **The transforms are pure functions.** `src/coverdrive/transform.py`
   takes a DataFrame and returns a DataFrame — no I/O, no S3 client, no
   filesystem. The tests in `tests/test_transform.py` exercise them
   directly from CSV fixtures, with no infrastructure. The boundary
   between "code that decides what data should look like" and "code that
   moves bytes around" is the single most useful seam in this pipeline.

3. **PCA is computed in dbt as a metric, not learned by a model.**
   The 2022 dissertation trained XGBoost to predict a PCA score derived
   from its own input features — target leakage. The fix is structural:
   PCA now lives in `dbt/macros/compute_pca.sql` as a deterministic
   linear combination, applied at warehouse build time. The model layer
   is gone. The reasoning is documented in full in
   [`adr-pca-leakage.md`](docs/adr-pca-leakage.md).

4. **DuckDB over a real warehouse, deliberately.** dbt-duckdb has the
   same SQL surface as Snowflake/BigQuery/Postgres for what this project
   needs, costs nothing, and reads Parquet on S3 natively. The same dbt
   project compiles against a managed warehouse with a profile change.
   Choosing DuckDB here is choosing to spend the complexity budget on
   the data model, not the infrastructure.

5. **The Terraform stops at the data-plane primitives.** It provisions
   S3, RDS, ECR, IAM, and CloudWatch — and stops. The ECS task
   definition is intentionally out of scope, because *which* compute
   layer (ECS Fargate / MWAA / EC2 / k8s) is environment-specific and
   encoding one of them into the same module conflates data plane with
   orchestration plane. The reasoning is in
   [`infra/terraform/README.md`](infra/terraform/README.md).

---

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python 3.11 |
| HTTP + parsing | `requests`, `beautifulsoup4`, `lxml`, `tenacity` |
| DataFrames | `pandas`, `pyarrow` |
| Storage | S3 / MinIO via `boto3` + `s3fs`, Parquet (Snappy) |
| Config | `pydantic`, `pydantic-settings`, YAML |
| Logging | `structlog` (JSON in prod, key-value in dev) |
| Quality | `pandera` |
| Warehouse | DuckDB |
| Transformation | dbt (`dbt-duckdb`) |
| Serving | FastAPI + Uvicorn |
| Orchestration | Airflow 2.9 (TaskFlow API) |
| Tests | pytest, `pytest-cov`, `moto` |
| Lint / format | ruff |
| Types | mypy (strict mode) |
| Infrastructure | Terraform 1.6+, AWS provider 5.x |
| CI | GitHub Actions |

---

## Tests & CI

```bash
make lint        # ruff format --check + ruff check
make typecheck   # mypy in strict mode
make test        # pytest with coverage (fails under 80%)
make ci          # all three + dbt parse
```

CI runs the same gates on every push and pull request. See
[`.github/workflows/ci.yml`](.github/workflows/ci.yml). Coverage XML is
uploaded as an artifact.

---

## Repository map

```
coverdrive/
├── README.md                          ← you are here
├── Makefile                           one-command lifecycle
├── docker-compose.yml                 MinIO + Postgres + Airflow
├── pyproject.toml                     strict ruff/mypy config
├── conf/pipeline.yaml                 every parameter, Pydantic-validated
├── src/coverdrive/                     ingestion, transform, quality, api
├── dbt/                               sources, staging, marts, PCA macro
├── airflow/dags/daily_refresh.py      orchestration
├── tests/                             pytest + moto + on-disk fixtures
├── infra/terraform/                   AWS module
├── docs/
│   ├── ARCHITECTURE.md                system design
│   └── adr-pca-leakage.md             the postmortem
└── .github/workflows/ci.yml
```

---

## Limitations

In the spirit of being more useful than impressive, what this project
does **not** do:

- It is **batch, not streaming.** The source is a daily-refresh stat
  table — there is nothing to stream.
- It is **single-source.** Only ESPNcricinfo. The right second source is
  CricSheet for ball-by-ball data; see "Future work" in `ARCHITECTURE.md`.
- It is **ODI-only.** Test and T20I are a parameterised next step.
- It is **read-only.** No write path on the API.
- The PCA loadings are **fixed at 2022 values.** Refresh policy is an
  annual recomputation gated by human review, not an automated step.
  Rationale in [`adr-pca-leakage.md`](docs/adr-pca-leakage.md) §
  "Risks accepted."
- The **AWS Terraform applies cleanly but stops at the data-plane
  primitives.** No ECS service is provisioned. The local
  `docker-compose.yml` is the running stack; the Terraform is the
  production-target shape. Reasoning in `infra/terraform/README.md`.

---

## About

I'm Nadeem Theba. I built the original version of this for my MSc at
the University of Hertfordshire in 2022. In 2026 I rebuilt it as a data
platform — and corrected the methodology flaw I'd shipped in the
dissertation. The story of that flaw, and the fix, is in
[`docs/adr-pca-leakage.md`](docs/adr-pca-leakage.md).

If you're a hiring manager: the most useful 10 minutes you can spend in
this repo are the ADR, then `src/coverdrive/transform.py` + its tests,
then `dbt/macros/compute_pca.sql`. That path tells you how I think about
correctness, separation of concerns, and analytics engineering in roughly
that order.

- LinkedIn: [linkedin.com/in/nadeem-theba](linkedin.com/in/nadeem-theba-602862208)
- Email: nadeemtheba8@gmail.com

---

## License

MIT — see [LICENSE](LICENSE).
