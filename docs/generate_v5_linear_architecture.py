import base64
import urllib.request

# v5 Executive Remediated Architecture Diagram (Strict 5-Column Horizontal Line Grid)
mermaid_code = """
flowchart LR
    classDef card_python fill:#FFFFFF,stroke:#3B82F6,stroke-width:2px,color:#0F172A;
    classDef card_s3 fill:#FFFFFF,stroke:#F59E0B,stroke-width:2px,color:#0F172A;
    classDef card_spark fill:#FFFFFF,stroke:#EA580C,stroke-width:2px,color:#0F172A;
    classDef card_dbt fill:#FFFFFF,stroke:#EF4444,stroke-width:2px,color:#0F172A;
    classDef card_athena fill:#FFFFFF,stroke:#0284C7,stroke-width:3px,color:#0F172A;
    classDef card_redis fill:#FFFFFF,stroke:#DC2626,stroke-width:2px,color:#0F172A;
    classDef card_fastapi fill:#FFFFFF,stroke:#10B981,stroke-width:2px,color:#0F172A;
    classDef card_pandera fill:#FFFFFF,stroke:#A855F7,stroke-width:2px,color:#0F172A;
    classDef card_glue fill:#FFFFFF,stroke:#059669,stroke-width:2px,color:#0F172A;
    classDef card_dlq fill:#FEF2F2,stroke:#DC2626,stroke-width:2px,color:#991B1B;
    classDef card_ops fill:#F8FAFC,stroke:#64748B,stroke-width:2px,color:#0F172A;

    subgraph AWS_REGION ["☁️ AWS CLOUD REGION (US-EAST-1) / PRIVATE VPC PERIMETER"]
        direction LR

        %% ZONE 1: INGESTION & INGRESS
        subgraph ZONE_1 ["ZONE 1: INGESTION & INGRESS"]
            direction TB
            Src1["📄 <b>Cricsheet Archives</b><br/><span style='color:#64748B;'>JSON / CSV Match Feeds</span>"]:::card_python
            Src2["🌐 <b>ESPNcricinfo</b><br/><span style='color:#64748B;'>HTML Scrapers</span>"]:::card_python
            Src3["🌤️ <b>Open-Meteo API</b><br/><span style='color:#64748B;'>Weather REST Feeds</span>"]:::card_python
            Ingest["🐍 <b>Amazon ECS Fargate</b><br/><span style='color:#64748B;'>Serverless Ingest Tasks</span>"]:::card_python

            Src1 --> Ingest
            Src2 --> Ingest
            Src3 --> Ingest
        end

        %% ZONE 2: BRONZE & QUALITY
        subgraph ZONE_2 ["ZONE 2: BRONZE & QUALITY"]
            direction TB
            Bronze["🪣 <b>Amazon S3 Bronze Lake</b><br/><span style='color:#64748B;'>Raw Parquet Storage</span>"]:::card_s3
            Pandera["🛡️ <b>Pandera Schema Guard</b><br/><span style='color:#64748B;'>Shift-Left Data Contracts</span>"]:::card_pandera
            DLQ["📥 <b>Amazon S3 DLQ Bucket</b><br/><span style='color:#991B1B;'>Quarantine Bad Data</span>"]:::card_dlq

            Bronze --> Pandera
            Pandera -.->|Schema Breach| DLQ
        end

        %% ZONE 3: SILVER LAKE & COMPUTE
        subgraph ZONE_3 ["ZONE 3: SILVER LAKE & COMPUTE"]
            direction TB
            Spark["⚡ <b>EMR Serverless Spark</b><br/><span style='color:#64748B;'>AQE Skew-Join Processing</span>"]:::card_spark
            Silver["🪣 <b>Amazon S3 Silver Lake</b><br/><span style='color:#64748B;'>Cleaned & Typed Parquet</span>"]:::card_s3

            Spark --> Silver
        end

        %% ZONE 4: GOLD LAKE & GOVERNANCE
        subgraph ZONE_4 ["ZONE 4: GOLD LAKE & GOVERNANCE"]
            direction TB
            dbt["🟧 <b>dbt-Athena Core 1.8</b><br/><span style='color:#64748B;'>42 Quality Assertions</span>"]:::card_dbt
            Gold["🪣 <b>Amazon S3 Gold Lake</b><br/><span style='color:#64748B;'>Enriched Player Marts</span>"]:::card_s3
            Glue["🗂️ <b>AWS Glue Data Catalog</b><br/><span style='color:#64748B;'>Centralized Metastore</span>"]:::card_glue

            dbt --> Gold
            Glue -.- Silver
            Glue -.- Gold
        end

        %% ZONE 5: SERVING & ANALYTICS
        subgraph ZONE_5 ["ZONE 5: SERVING & ANALYTICS"]
            direction TB
            Athena["☁️ <b>Amazon Athena SQL Engine</b><br/><span style='color:#64748B;'>Serverless Query Engine</span>"]:::card_athena
            Redis["🔴 <b>Amazon ElastiCache Redis</b><br/><span style='color:#64748B;'>Sub-10ms Hot Query Cache</span>"]:::card_redis
            FastAPI["⚡ <b>FastAPI on ECS Fargate</b><br/><span style='color:#64748B;'>Stateless REST API</span>"]:::card_fastapi

            Athena --> Redis
            Redis --> FastAPI
        end

        %% HORIZONTAL CHAINING BETWEEN NODES TO ENFORCE STRICT 1-ROW ALIGNMENT
        Ingest ==>|1. Raw Parquet| Bronze
        Pandera ==>|2. Validated Data| Spark
        Silver ==>|3. Cleaned Models| dbt
        Gold ==>|4. Gold Scans| Athena

        %% BOTTOM HORIZONTAL BANNER: OPERATIONAL PLANE
        subgraph OPS_PLANE ["⚙️ CROSS-CUTTING OPERATIONAL PLANE (ORCHESTRATION, SECURITY, OBSERVABILITY & ENCRYPTION)"]
            direction LR
            MWAA["🌀 <b>AWS MWAA Airflow</b><br/><span style='color:#64748B;'>Managed DAG Orchestration</span>"]:::card_ops
            SEC["🔒 <b>AWS Secrets Manager & KMS</b><br/><span style='color:#64748B;'>IAM Roles & SSE-KMS Encryption</span>"]:::card_ops
            OBS["📊 <b>Amazon CloudWatch & X-Ray</b><br/><span style='color:#64748B;'>APM Tracing & SNS Alerts</span>"]:::card_ops
        end

        %% CONTROL CONNECTORS DIRECTLY LINKED FROM MWAA TO INGEST, BRONZE, SPARK, DBT IN STRAIGHT PATHS
        MWAA .->|Control Trigger| Ingest
        MWAA .->|Control Trigger| Bronze
        MWAA .->|Control Trigger| Spark
        MWAA .->|Control Trigger| dbt
    end
"""

# Base64 GET encoding with neutral theme
encoded = base64.b64encode(mermaid_code.encode("utf-8")).decode("utf-8")
url_svg = f"https://mermaid.ink/svg/{encoded}?theme=neutral"
url_png = f"https://mermaid.ink/img/{encoded}?theme=neutral"

req_svg = urllib.request.Request(url_svg, headers={"User-Agent": "Mozilla/5.0"})
req_png = urllib.request.Request(url_png, headers={"User-Agent": "Mozilla/5.0"})

try:
    with urllib.request.urlopen(req_svg) as resp:
        svg_bytes = resp.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v5.svg", "wb"
        ) as f:
            f.write(svg_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_architecture.svg",
            "wb",
        ) as f:
            f.write(svg_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v4.svg", "wb"
        ) as f:
            f.write(svg_bytes)
    print("v5 Linear SVG saved!")
except Exception as e:
    print("SVG Error:", e)

try:
    with urllib.request.urlopen(req_png) as resp:
        png_bytes = resp.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v5.png", "wb"
        ) as f:
            f.write(png_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_architecture_diagram.png",
            "wb",
        ) as f:
            f.write(png_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v4.png", "wb"
        ) as f:
            f.write(png_bytes)
    print("v5 Linear PNG saved!")
except Exception as e:
    print("PNG Error:", e)
