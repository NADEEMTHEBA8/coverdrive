# Coverdrive: Event-Driven Analytics & Regulatory Pipeline

![Python](https://img.shields.io/badge/python-3.11-blue)
![PySpark](https://img.shields.io/badge/pyspark-3.5-E25A1C)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![DuckDB](https://img.shields.io/badge/duckdb-1.0-FFF000)
![Airflow](https://img.shields.io/badge/airflow-2.9-017CEE)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20IAM-FF9900)

## 📖 Executive Overview

**Coverdrive** is a production-grade, event-driven data lakehouse platform built using **DuckDB**, **dbt**, **PySpark**, and **AWS S3**.

While utilizing high-dimensional cricket sports telemetry as its primary domain dataset, the platform architecture enforces strict production standards expected in **enterprise financial services and telemetry systems**:
- **Shift-Left Data Contracts:** Programmatic data validation gates via `pandera` preventing contaminated metrics from reaching analytical models.
- **Distributed Skew Mitigation:** Custom PySpark key-salting algorithms to eliminate executor memory imbalance during high-cardinality joins.
- **Decoupled Architecture:** Ephemeral compute (DuckDB/PySpark) decoupled from zero-maintenance object storage (AWS S3 Parquet/Delta Lake).

---

## 🏗 System Architecture

```mermaid
flowchart LR
    subgraph Ingestion [1. Ingestion: Idempotent Fetch]
        A[ESPNcricinfo / Cricsheet] -->|Python| B[(AWS S3: Bronze)]
    end

    subgraph Storage [2. Lakehouse Storage]
        B -.->|Clean Parquet| C[(AWS S3: Silver)]
        C -.->|Salted Enriched| D[(AWS S3: Gold)]
    end

    subgraph Processing [3. Quality Gates & Compute]
        B -->|PySpark Key Salting| C
        C -->|Pandera Contracts| D
    end

    subgraph Serving [4. Semantic Modeling]
        D -->|dbt| E[(DuckDB Warehouse)]
        E -->|FastAPI| F[Analytical REST API]
    end
```

---

## 📸 Production Proof & Infrastructure Validation

The pipeline includes built-in verification artifacts demonstrating live service execution:

* **MinIO / AWS S3 Storage Partitions:**
  ![MinIO Partitions](docs/assets/minio_partitions.png)

* **Apache Airflow Orchestration DAG:**
  ![Airflow Execution](docs/assets/airflow_dag_success.png)

* **FastAPI Analytics Endpoint:**
  ![Swagger REST Response](docs/assets/api_batsmen_response.png)

---

## 🛠 Tech Stack & Rationale

| Component | Technology | Architectural Rationale |
| :--- | :--- | :--- |
| **Infrastructure** | **Terraform** | Declarative provisioning of S3 buckets, IAM policies, and VPC storage endpoints. |
| **Storage** | **AWS S3 / MinIO** | Immutable Medallion storage (Bronze/Silver/Gold) decoupled from compute. |
| **Transform** | **PySpark & DuckDB** | PySpark for cluster-distributed joins; DuckDB for ultra-fast local OLAP dbt execution. |
| **Modeling** | **dbt (Data Build Tool)** | Modular SQL transformation models, schema tests, and lineage generation. |
| **Orchestration** | **Apache Airflow** | Automated daily DAG execution with strict task dependency tracking and alerting. |

---

## 🔒 Governance & Data Quality (Shift-Left)

Before Silver data promotes to Gold analytical models, `pandera` data contracts validate:
- **Null Ratios & Volume Ceilings:** Validates row count baselines and enforces missing data thresholds.
- **Domain Invariants:** Asserts bounded numeric ranges (e.g. valid career year spans, non-negative runs/wickets).
- **Circuit Breaker:** Halts the Airflow DAG immediately on schema violations to protect downstream dashboards.

---

## 🚀 Execution Instructions

### 1. Configure Environment
```bash
cp .env.example .env.local
```

### 2. Run Test Suite & Quality Gates
```bash
make test
```

### 3. Trigger Local Medallion Pipeline
```bash
make demo
```

---

## 🏛 Key Technical Trade-Offs

### 1. PySpark Key-Salting for Skew Mitigation
Joining wide fact tables with skewed player dimensions can cause executor out-of-memory errors in Spark. By adding a randomized key salt (`_SALT_BUCKETS = 10`), records distribute evenly across Spark partitions during left outer joins.

### 2. DuckDB + dbt for Low-Cost Analytics
Rather than maintaining an expensive 24/7 cloud warehouse for medium-scale analytics, Coverdrive executes dbt directly against DuckDB querying Parquet on S3 — delivering sub-second queries at near-zero infrastructure cost.
