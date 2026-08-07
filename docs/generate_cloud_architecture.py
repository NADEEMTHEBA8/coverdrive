import base64
import urllib.request

# Pure AWS Cloud Production Architecture Diagram (100% Cloud Managed Stack)
mermaid_code = """
flowchart LR
    %% Official Crisp Card Palette with Brand Borders
    classDef aws_s3 fill:#FFFFFF,stroke:#D97706,stroke-width:4px,color:#000000;
    classDef aws_athena fill:#FFFFFF,stroke:#0284C7,stroke-width:4px,color:#000000;
    classDef spark fill:#FFFFFF,stroke:#E25A1C,stroke-width:4px,color:#000000;
    classDef dbt fill:#FFFFFF,stroke:#FF694B,stroke-width:4px,color:#000000;
    classDef airflow fill:#FFFFFF,stroke:#017CEE,stroke-width:4px,color:#000000;
    classDef redis fill:#FFFFFF,stroke:#DC2626,stroke-width:4px,color:#000000;
    classDef fastapi fill:#FFFFFF,stroke:#059669,stroke-width:4px,color:#000000;
    classDef python fill:#FFFFFF,stroke:#2563EB,stroke-width:4px,color:#000000;
    classDef pandera fill:#FFFFFF,stroke:#C026D3,stroke-width:4px,color:#000000;
    classDef glue fill:#FFFFFF,stroke:#7C3AED,stroke-width:4px,color:#000000;

    subgraph AWS_VPC ["🛡️ AWS PRIVATE VPC (SECURITY & IAM PERIMETER)"]
        direction LR

        %% STEP 1: INGESTION
        subgraph S1 ["STEP 1: INGESTION"]
            direction TB
            Src1["📄 <b style='font-size:16px;color:#1E293B;'>CRICSHEET DATA</b><br/><span style='color:#64748B;'>Match JSON / CSV Feeds</span>"]:::python
            Src2["🌐 <b style='font-size:16px;color:#1E293B;'>ESPN CRICINFO</b><br/><span style='color:#64748B;'>HTML Scrapers</span>"]:::python
            Ingest["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg' width='80'/><br/><b style='font-size:18px;color:#2563EB;'>AWS ECS FARGATE</b><br/><span style='color:#475569;'>Python Ingest Task</span>"]:::python
            Src1 --> Ingest
            Src2 --> Ingest
        end

        %% STEP 2: BRONZE & QUALITY
        subgraph S2 ["STEP 2: BRONZE & QUALITY"]
            direction TB
            Bronze["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='80'/><br/><b style='font-size:18px;color:#D97706;'>AWS S3 BRONZE LAKE</b><br/><span style='color:#475569;'>Raw Parquet Storage</span>"]:::aws_s3
            Airflow["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/apacheairflow/apacheairflow-original.svg' width='80'/><br/><b style='font-size:18px;color:#017CEE;'>AWS MWAA AIRFLOW</b><br/><span style='color:#475569;'>Managed DAG Orchestrator</span>"]:::airflow
            Pandera["🛡️ <br/><b style='font-size:18px;color:#C026D3;'>PANDERA CONTRACTS</b><br/><span style='color:#475569;'>Shift-Left Schema Guard</span>"]:::pandera
            DLQ["📥 <br/><b style='font-size:18px;color:#DC2626;'>S3 DLQ BUCKET</b><br/><span style='color:#475569;'>Quarantine Bad Data</span>"]:::redis

            Airflow --> Pandera
            Pandera -.->|Invalid Schema| DLQ
        end

        %% STEP 3: COMPUTE & GOVERNANCE
        subgraph S3 ["STEP 3: COMPUTE & GOVERNANCE"]
            direction TB
            Spark["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/apachespark/apachespark-original.svg' width='80'/><br/><b style='font-size:18px;color:#E25A1C;'>EMR SERVERLESS SPARK</b><br/><span style='color:#475569;'>AQE Skew-Join Processing</span>"]:::spark
            Glue["🗂️ <br/><b style='font-size:18px;color:#7C3AED;'>AWS GLUE CATALOG</b><br/><span style='color:#475569;'>Centralized Metastore</span>"]:::glue
            Silver["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='80'/><br/><b style='font-size:18px;color:#D97706;'>AWS S3 SILVER LAKE</b><br/><span style='color:#475569;'>Cleaned & Typed Parquet</span>"]:::aws_s3
            dbt["🟧 <br/><b style='font-size:18px;color:#FF694B;'>dbt-ATHENA CORE 1.8</b><br/><span style='color:#475569;'>42/42 Passed Quality Tests</span>"]:::dbt
            Gold["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='80'/><br/><b style='font-size:18px;color:#D97706;'>AWS S3 GOLD LAKE</b><br/><span style='color:#475569;'>Enriched Player Marts</span>"]:::aws_s3

            Spark --> Silver
            Silver --> dbt
            dbt --> Gold
            Glue -. Metastore Sync .-> Silver
            Glue -. Metastore Sync .-> Gold
        end

        %% STEP 4: SERVING & ANALYTICS
        subgraph S4 ["STEP 4: SERVING & ANALYTICS"]
            direction TB
            Athena["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='80'/><br/><b style='font-size:18px;color:#0284C7;'>AWS ATHENA SQL</b><br/><span style='color:#475569;'>Serverless Query Engine</span>"]:::aws_athena
            Redis["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original.svg' width='80'/><br/><b style='font-size:18px;color:#DC2626;'>ELASTICACHE REDIS</b><br/><span style='color:#475569;'>Sub-10ms Hot Query Cache</span>"]:::redis
            FastAPI["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/fastapi/fastapi-original.svg' width='80'/><br/><b style='font-size:18px;color:#059669;'>FASTAPI ON ECS</b><br/><span style='color:#475569;'>Stateless REST API</span>"]:::fastapi

            Athena --> Redis
            Redis --> FastAPI
        end

        %% SEQUENTIAL CONNECTIONS
        Ingest -->|1. Write Raw| Bronze
        Bronze -->|2. Validate| Pandera
        Pandera -->|3. Clean & Salt| Spark
        Gold -->|4. Query Gold| Athena
    end
"""

encoded = base64.b64encode(mermaid_code.encode("utf-8")).decode("utf-8")
url = f"https://mermaid.ink/svg/{encoded}"

req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
try:
    with urllib.request.urlopen(req) as response:
        svg_data = response.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_architecture.svg",
            "wb",
        ) as f:
            f.write(svg_data)
        print("Successfully saved Pure AWS Cloud SVG architecture diagram!")
except Exception as e:
    print("Error downloading SVG:", e)
