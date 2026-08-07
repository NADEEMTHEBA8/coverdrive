import base64
import urllib.request

# Dual-Target Side-by-Side Comparison Architecture Diagram
mermaid_code = """
flowchart TB
    %% Class Styles
    classDef local_box fill:#FFFFFF,stroke:#2563EB,stroke-width:2px,color:#0F172A;
    classDef aws_box fill:#FFFFFF,stroke:#F59E0B,stroke-width:2px,color:#0F172A;
    classDef sub_card fill:#F8FAFC,stroke:#CBD5E1,stroke-width:1px,color:#0F172A;

    subgraph TARGET_LOCAL ["💻 LOCAL DEVELOPMENT TARGET ($0 CLOUD SPEND - LOCAL EMULATION)"]
        direction LR
        L_Ingest["🐍 <b>Python Extractor</b><br/><span style='color:#64748B;'>Local Scripts</span>"]:::sub_card
        L_MinIO["🪣 <b>MinIO S3</b><br/><span style='color:#64748B;'>Local Storage</span>"]:::sub_card
        L_Airflow["🌀 <b>Docker Airflow</b><br/><span style='color:#64748B;'>Local Orchestration</span>"]:::sub_card
        L_Spark["⚡ <b>Local PySpark</b><br/><span style='color:#64748B;'>N=16 Salting</span>"]:::sub_card
        L_dbt["🟧 <b>dbt-duckdb</b><br/><span style='color:#64748B;'>42/42 Quality Tests</span>"]:::sub_card
        L_DuckDB["🦆 <b>DuckDB Engine</b><br/><span style='color:#64748B;'>warehouse.duckdb</span>"]:::sub_card
        L_FastAPI["⚡ <b>Local FastAPI</b><br/><span style='color:#64748B;'>Direct Read</span>"]:::sub_card

        L_Ingest --> L_MinIO
        L_MinIO --> L_Airflow
        L_Airflow --> L_Spark
        L_Spark --> L_dbt
        L_dbt --> L_DuckDB
        L_DuckDB --> L_FastAPI
    end

    subgraph TARGET_AWS ["☁️ PRODUCTION AWS CLOUD TARGET (100% MANAGED ENTERPRISE STACK)"]
        direction LR

        subgraph VPC ["🛡️ AWS PRIVATE VPC (SECURITY & IAM PERIMETER)"]
            direction LR

            subgraph AWS_S1 ["1. INGESTION"]
                direction TB
                A_Src["📄 <b>3 Feeds</b><br/><span style='color:#64748B;'>Cricsheet, ESPN, Weather</span>"]:::sub_card
                A_Ingest["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg' width='30'/><br/><b style='color:#2563EB;'>AWS ECS Fargate</b><br/><span style='color:#64748B;'>Ingest Tasks</span>"]:::sub_card
                A_Src --> A_Ingest
            end

            subgraph AWS_S2 ["2. BRONZE & QUALITY"]
                direction TB
                A_Bronze["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='30'/><br/><b style='color:#D97706;'>S3 Bronze Lake</b><br/><span style='color:#64748B;'>Raw Parquet</span>"]:::sub_card
                A_MWAA["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/apacheairflow/apacheairflow-original.svg' width='30'/><br/><b style='color:#017CEE;'>AWS MWAA Airflow</b><br/><span style='color:#64748B;'>DAG Scheduler</span>"]:::sub_card
                A_Pandera["🛡️ <b>Pandera Contracts</b><br/><span style='color:#64748B;'>Shift-Left Guard</span>"]:::sub_card
                A_DLQ["📥 <b style='color:#DC2626;'>S3 DLQ Bucket</b><br/><span style='color:#991B1B;'>Quarantine Sink</span>"]:::sub_card

                A_MWAA --> A_Pandera
                A_Pandera -.->|Invalid Schema| A_DLQ
            end

            subgraph AWS_S3_STAGE ["3. COMPUTE & GOVERNANCE"]
                direction TB
                A_EMR["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/apachespark/apachespark-original.svg' width='30'/><br/><b style='color:#EA580C;'>EMR Spark AQE</b><br/><span style='color:#64748B;'>Dynamic Join Skew</span>"]:::sub_card
                A_Glue["🗂️ <b>Glue Catalog</b><br/><span style='color:#64748B;'>Metastore Sync</span>"]:::sub_card
                A_Silver["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='30'/><br/><b style='color:#D97706;'>S3 Silver Lake</b><br/><span style='color:#64748B;'>Cleaned Parquet</span>"]:::sub_card
                A_dbt["🟧 <b style='color:#EF4444;'>dbt-Athena 1.8</b><br/><span style='color:#64748B;'>42 Quality Tests</span>"]:::sub_card
                A_Gold["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='30'/><br/><b style='color:#D97706;'>S3 Gold Lake</b><br/><span style='color:#64748B;'>Enriched Marts</span>"]:::sub_card

                A_EMR --> A_Silver
                A_Silver --> A_dbt
                A_dbt --> A_Gold
                A_Glue -. Sync .-> A_Silver
                A_Glue -. Sync .-> A_Gold
            end

            subgraph AWS_S4 ["4. SERVING & ANALYTICS"]
                direction TB
                A_Athena["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='30'/><br/><b style='color:#0284C7;'>AWS Athena SQL</b><br/><span style='color:#64748B;'>Serverless Query</span>"]:::sub_card
                A_Redis["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original.svg' width='30'/><br/><b style='color:#DC2626;'>ElastiCache Redis</b><br/><span style='color:#64748B;'>Sub-10ms Cache</span>"]:::sub_card
                A_FastAPI["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/fastapi/fastapi-original.svg' width='30'/><br/><b style='color:#10B981;'>FastAPI on ECS</b><br/><span style='color:#64748B;'>Stateless REST API</span>"]:::sub_card

                A_Athena --> A_Redis
                A_Redis --> A_FastAPI
            end

            A_Ingest --> A_Bronze
            A_Bronze --> A_Pandera
            A_Pandera --> A_EMR
            A_Gold --> A_Athena
        end
    end
"""

encoded = base64.b64encode(mermaid_code.encode("utf-8")).decode("utf-8")
url_svg = f"https://mermaid.ink/svg/{encoded}?theme=neutral"
url_png = f"https://mermaid.ink/img/{encoded}?theme=neutral"

req_svg = urllib.request.Request(url_svg, headers={"User-Agent": "Mozilla/5.0"})
req_png = urllib.request.Request(url_png, headers={"User-Agent": "Mozilla/5.0"})

try:
    with urllib.request.urlopen(req_svg) as resp:
        svg_bytes = resp.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_dual_target_architecture.svg",
            "wb",
        ) as f:
            f.write(svg_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_architecture.svg",
            "wb",
        ) as f:
            f.write(svg_bytes)
    print("Dual-Target SVG saved successfully!")
except Exception as e:
    print("SVG Error:", e)

try:
    with urllib.request.urlopen(req_png) as resp:
        png_bytes = resp.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_dual_target_architecture.png",
            "wb",
        ) as f:
            f.write(png_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_architecture_diagram.png",
            "wb",
        ) as f:
            f.write(png_bytes)
    print("Dual-Target PNG saved successfully!")
except Exception as e:
    print("PNG Error:", e)
