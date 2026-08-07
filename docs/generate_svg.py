import base64
import urllib.request

# Crisp White Cards with High-Res Brand Colored Logos (80px) and Thick Technology Border Lines
mermaid_code = """
flowchart LR
    %% Crisp Card Styles with Brand Colored Borders & Shadows
    classDef spark fill:#FFFFFF,stroke:#E25A1C,stroke-width:4px,color:#000000;
    classDef dbt fill:#FFFFFF,stroke:#FF694B,stroke-width:4px,color:#000000;
    classDef airflow fill:#FFFFFF,stroke:#017CEE,stroke-width:4px,color:#000000;
    classDef duckdb fill:#FFFFFF,stroke:#EAB308,stroke-width:4px,color:#000000;
    classDef aws_s3 fill:#FFFFFF,stroke:#D97706,stroke-width:4px,color:#000000;
    classDef aws_athena fill:#FFFFFF,stroke:#0284C7,stroke-width:4px,color:#000000;
    classDef fastapi fill:#FFFFFF,stroke:#059669,stroke-width:4px,color:#000000;
    classDef python fill:#FFFFFF,stroke:#2563EB,stroke-width:4px,color:#000000;
    classDef pandera fill:#FFFFFF,stroke:#C026D3,stroke-width:4px,color:#000000;

    %% 1. INGESTION (Far Left)
    subgraph S1 ["STEP 1: INGESTION"]
        direction TB
        Src1["📄 <b style='font-size:16px;color:#1E293B;'>CRICSHEET DATA</b><br/><span style='color:#64748B;'>Raw Match JSON / CSV</span>"]:::python
        Src2["🌐 <b style='font-size:16px;color:#1E293B;'>ESPN CRICINFO</b><br/><span style='color:#64748B;'>HTML Telemetry Scrapers</span>"]:::python
        Ingest["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg' width='80'/><br/><b style='font-size:18px;color:#2563EB;'>PYTHON EXTRACTOR</b><br/><span style='color:#475569;'>Idempotent Batch Pipeline</span>"]:::python
        Src1 --> Ingest
        Src2 --> Ingest
    end

    %% 2. BRONZE & CONTROL PLANE (Middle Left)
    subgraph S2 ["STEP 2: BRONZE & QUALITY"]
        direction TB
        Bronze["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='80'/><br/><b style='font-size:18px;color:#D97706;'>AWS S3 BRONZE LAKE</b><br/><span style='color:#475569;'>Raw Unprocessed Payloads</span>"]:::aws_s3
        Airflow["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/apacheairflow/apacheairflow-original.svg' width='80'/><br/><b style='font-size:18px;color:#017CEE;'>APACHE AIRFLOW</b><br/><span style='color:#475569;'>DAG Orchestrator & Scheduler</span>"]:::airflow
        Pandera["🛡️ <br/><b style='font-size:18px;color:#C026D3;'>PANDERA CONTRACTS</b><br/><span style='color:#475569;'>Shift-Left Schema Guard</span>"]:::pandera
        Airflow --> Pandera
    end

    %% 3. TRANSFORM & SILVER/GOLD (Middle Right)
    subgraph S3 ["STEP 3: COMPUTE & MATURATION"]
        direction TB
        Spark["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/apachespark/apachespark-original.svg' width='80'/><br/><b style='font-size:18px;color:#E25A1C;'>APACHE PYSPARK 3.5.1</b><br/><span style='color:#475569;'>Key-Salting Skew Reduction N=16</span>"]:::spark
        Silver["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='80'/><br/><b style='font-size:18px;color:#D97706;'>AWS S3 SILVER LAKE</b><br/><span style='color:#475569;'>1.26M Cleaned & Typed Deliveries</span>"]:::aws_s3
        dbt["🟧 <br/><b style='font-size:18px;color:#FF694B;'>dbt CORE 1.8 ENGINE</b><br/><span style='color:#475569;'>42/42 Passed Data Quality Tests</span>"]:::dbt
        Gold["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='80'/><br/><b style='font-size:18px;color:#D97706;'>AWS S3 GOLD LAKE</b><br/><span style='color:#475569;'>Enriched Player Performance Marts</span>"]:::aws_s3

        Spark --> Silver
        Silver --> dbt
        dbt --> Gold
    end

    %% 4. SERVING & ANALYTICS (Far Right)
    subgraph S4 ["STEP 4: SERVING & ANALYTICS"]
        direction TB
        DuckDB["🦆 <br/><b style='font-size:18px;color:#CA8A04;'>DUCKDB OLAP ENGINE</b><br/><span style='color:#475569;'>High-Performance Local Analytics</span>"]:::duckdb
        Athena["☁️ <br/><b style='font-size:18px;color:#0284C7;'>AWS ATHENA SQL</b><br/><span style='color:#475569;'>Serverless 515ms S3 Parquet Scans</span>"]:::aws_athena
        FastAPI["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/fastapi/fastapi-original.svg' width='80'/><br/><b style='font-size:18px;color:#059669;'>FASTAPI REST API</b><br/><span style='color:#475569;'>High-Speed Analytics Serving Endpoint</span>"]:::fastapi

        DuckDB --> FastAPI
    end

    %% STRICT LEFT-TO-RIGHT SEQUENTIAL CONNECTIONS
    Ingest -->|1. Write Parquet| Bronze
    Bronze -->|2. Validate & Read| Pandera
    Pandera -->|3. Salted Join| Spark
    Gold -->|4. Load Marts| DuckDB
    Gold -->|5. External Queries| Athena
"""

encoded = base64.b64encode(mermaid_code.encode("utf-8")).decode("utf-8")
url = f"https://mermaid.ink/svg/{encoded}"

req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
try:
    with urllib.request.urlopen(req) as response:
        svg_data = response.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_architecture.svg", "wb"
        ) as f:
            f.write(svg_data)
        print("Successfully saved large logo SVG architecture diagram!")
except Exception as e:
    print("Error downloading SVG:", e)
