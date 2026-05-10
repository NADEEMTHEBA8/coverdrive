# Coverdrive — production-grade cricket data platform
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
	cd $(DBT_DIR) && ../$(DBT) deps

clean:  ## Remove venv, caches, build artifacts
	rm -rf $(VENV) .pytest_cache .ruff_cache .mypy_cache
	rm -rf build dist *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs $(DBT_DIR)/dbt_packages

# ─── Local infra ──────────────────────────────────────────────────────
up:  ## Start MinIO, Postgres, Airflow via docker-compose
	docker compose up -d
	@echo ""
	@echo "MinIO console:   http://localhost:9101  (minioadmin / minioadmin)"
	@echo "Airflow UI:      http://localhost:8180  (admin / admin)"
	@echo "API (once up):   http://localhost:8000/docs"

down:  ## Stop all containers
	docker compose down

logs:  ## Tail logs from all services
	docker compose logs -f --tail=100

# ─── Demo path ────────────────────────────────────────────────────────
seed:  ## Load fixture CSVs into Bronze as if freshly scraped
	$(PY) -m coverdrive.ingestion --mode=fixtures

ingest:  ## Run a fresh scrape from ESPNcricinfo into Bronze
	$(PY) -m coverdrive.ingestion --mode=scrape

transform:  ## Bronze → Silver: dedupe, type-cast, conform
	$(PY) -m coverdrive.transform

quality:  ## Run Pandera quality gates on Silver (halts on failure)
	$(PY) -m coverdrive.quality

dbt-build:  ## Run dbt: build all models against Silver Parquet
	cd $(DBT_DIR) && ../$(DBT) build --target=$(DBT_TARGET)

dbt-test:  ## Run dbt tests only
	cd $(DBT_DIR) && ../$(DBT) test --target=$(DBT_TARGET)

dbt-docs:  ## Generate and serve dbt docs (lineage graph)
	cd $(DBT_DIR) && ../$(DBT) docs generate --target=$(DBT_TARGET)
	cd $(DBT_DIR) && ../$(DBT) docs serve --target=$(DBT_TARGET)

api:  ## Start the FastAPI service locally
	$(PY) -m coverdrive.api

demo: seed transform quality dbt-build  ## End-to-end demo on fixture data
	@echo ""
	@echo "✔ Demo complete. Warehouse populated at data/warehouse.duckdb"
	@echo "  Run 'make api' then visit http://localhost:8000/docs"

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
