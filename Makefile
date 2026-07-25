# Coverdrive — cricket analytics lakehouse
# Run `make help` to see what's available.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Auto-load .env so env vars are available to every target.
# Lines must be KEY=value with no spaces around the =.
# Comments (#) at end of line are NOT supported by `include` — keep them on
# their own line in .env.
ifneq (,$(wildcard .env))
    include .env
    export
endif

# ─── Environment ──────────────────────────────────────────────────────
PYTHON := python3.11
VENV := .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
MYPY := $(VENV)/bin/mypy
DBT := $(VENV)/bin/dbt
DBT_DIR := dbt

# Pass --target=ci to use the postgres profile in CI
DBT_TARGET ?= dev

.PHONY: help install up down logs clean seed lint typecheck test \
        ingest transform quality dbt-build dbt-test dbt-docs api demo

help:  ## Show this help
	@awk 'BEGIN {FS=":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n\n"} \
		/^[a-zA-Z_-]+:.*?##/ {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ─── Setup ────────────────────────────────────────────────────────────
install:  ## Create venv and install all deps (incl. dbt + dev)
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev,dbt]"
	$(VENV)/bin/pre-commit install
	$(DBT) deps --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

clean:  ## Remove venv, caches, build artifacts
	rm -rf $(VENV) .pytest_cache .ruff_cache .mypy_cache
	rm -rf build dist *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs $(DBT_DIR)/dbt_packages

# ─── Local infra ──────────────────────────────────────────────────────
up:  ## Start MinIO, Postgres, Airflow, API via docker-compose
	docker compose up -d --build
	@echo ""
	@echo "MinIO console:   http://localhost:9101  (minioadmin / minioadmin)"
	@echo "Airflow UI:      http://localhost:8180  (admin / admin)"
	@echo "API:             http://localhost:8000/docs  (degraded until make dbt-build)"

down:  ## Stop all containers
	docker compose down

logs:  ## Tail logs from all services
	docker compose logs -f --tail=100

# ─── Demo path ────────────────────────────────────────────────────────
seed:  ## Load fixture CSVs into Bronze as if freshly scraped
	$(PY) -m coverdrive.extract.espn_html_extractor --mode=fixtures

ingest:  ## Run a fresh scrape from ESPNcricinfo into Bronze
	$(PY) -m coverdrive.extract.espn_html_extractor --mode=scrape

ingest-cricsheet:  ## Download Cricsheet JSON matches into Bronze
	$(PY) -m coverdrive.extract.cricsheet_archive

ingest-weather:  ## Download historical weather for Cricsheet matches into Bronze
	$(PY) -m coverdrive.extract.open_meteo_api

transform:  ## Bronze → Silver: dedupe, type-cast, conform using Pandas
	$(PY) -m coverdrive.transform.schema_conform

transform-cricsheet:  ## Flatten Cricsheet JSON into Silver Parquet using PySpark
	$(PY) -m src.coverdrive.processing.silver_cricsheet_etl \
		--bronze-path="s3a://$(COVERDRIVE_S3_BUCKET)/bronze/cricsheet/" \
		--silver-matches="s3a://$(COVERDRIVE_S3_BUCKET)/silver/cricsheet_matches/" \
		--silver-balls="s3a://$(COVERDRIVE_S3_BUCKET)/silver/cricsheet_balls/"

transform-weather:  ## Process Weather JSON into Silver Parquet using PySpark
	$(PY) -m src.coverdrive.processing.silver_weather_etl \
		--bronze-path="s3a://$(COVERDRIVE_S3_BUCKET)/bronze/weather/*.json" \
		--silver-path="s3a://$(COVERDRIVE_S3_BUCKET)/silver/weather/"

enrich:  ## Silver → Gold: PySpark key-salted joins for skewed data
	export BRONZE_S3_PATH="s3a://$(COVERDRIVE_S3_BUCKET)/bronze/" && \
	export SILVER_S3_PATH="s3a://$(COVERDRIVE_S3_BUCKET)/silver/" && \
	export GOLD_S3_PATH="s3a://$(COVERDRIVE_S3_BUCKET)/gold/" && \
	$(PY) -m src.coverdrive.processing.silver_pyspark_etl

quality:  ## Run Pandera quality gates on Silver (halts on failure)
	$(PY) -m coverdrive.contracts.pandera_gates

dbt-build:  ## Run dbt pipelines locally to populate DuckDB
	$(DBT) deps --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)
	$(DBT) build --target=$(DBT_TARGET) --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-test:  ## Run dbt tests
	$(DBT) test --target=$(DBT_TARGET) --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-docs:  ## Generate and serve dbt docs
	$(DBT) docs generate --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)
	$(DBT) docs serve --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target=$(DBT_TARGET)

api:  ## Start the FastAPI service locally
	$(PY) -m coverdrive.api

demo:  ## End-to-end demo: start services, seed fixtures, run full pipeline
	$(MAKE) up
	@echo "Waiting for MinIO..."
	@until curl -sf http://localhost:9100/minio/health/live >/dev/null 2>&1; do sleep 2; done
	$(MAKE) seed
	$(MAKE) ingest-cricsheet
	$(MAKE) ingest-weather
	$(MAKE) transform
	$(MAKE) transform-cricsheet
	$(MAKE) transform-weather
	$(MAKE) enrich
	$(MAKE) quality
	$(MAKE) dbt-build
	@echo ""
	@echo "✔ Demo complete."
	@echo "  Warehouse:   data/warehouse.duckdb"
	@echo "  API docs:    http://localhost:8000/docs"
	@echo "  MinIO:       http://localhost:9101  (minioadmin / minioadmin)"
	@echo "  Airflow:     http://localhost:8180  (admin / admin)"

# ─── Quality gates ────────────────────────────────────────────────────
lint:  ## ruff lint + format check
	$(RUFF) check src tests airflow
	$(RUFF) format --check src tests airflow

format:  ## ruff format (writes changes)
	$(RUFF) format src tests airflow
	$(RUFF) check --fix src tests airflow

typecheck:  ## mypy strict
	$(MYPY) src

test:  ## Run unit tests with coverage
	$(PYTEST) tests/

ci: lint typecheck test  ## Everything CI runs — must pass before merge
	cd $(DBT_DIR) && ../$(DBT) parse --target=$(DBT_TARGET)
