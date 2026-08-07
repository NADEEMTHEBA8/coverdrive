import base64
import urllib.request

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

        subgraph ZONE_1 ["ZONE 1: INGESTION & INGRESS"]
            direction TB
            Src1["📄 <b>Cricsheet Archives</b><br/><span style='color:#64748B;'>JSON / CSV Match Feeds</span>"]:::card_python
            Src2["🌐 <b>ESPNcricinfo</b><br/><span style='color:#64748B;'>HTML Scrapers</span>"]:::card_python
            Src3["🌤️ <b>Open-Meteo API</b><br/><span style='color:#64748B;'>Weather REST Feeds</span>"]:::card_python
            Ingest["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg' width='30'/><br/><b style='color:#2563EB;'>Amazon ECS Fargate</b><br/><span style='color:#64748B;'>Serverless Ingest Tasks</span>"]:::card_python

            Src1 --> Ingest
            Src2 --> Ingest
            Src3 --> Ingest
        end

        subgraph ZONE_2 ["ZONE 2: BRONZE & QUALITY GATE"]
            direction TB
            Bronze["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='30'/><br/><b style='color:#D97706;'>Amazon S3 Bronze Lake</b><br/><span style='color:#64748B;'>Raw Parquet Storage</span>"]:::card_s3
            Pandera["🛡️<br/><b style='color:#C026D3;'>Pandera Schema Guard</b><br/><span style='color:#64748B;'>Shift-Left Data Contracts</span>"]:::card_pandera
            DLQ["📥<br/><b style='color:#DC2626;'>Amazon S3 DLQ Bucket</b><br/><span style='color:#991B1B;'>Quarantine Bad Data</span>"]:::card_dlq

            Bronze --> Pandera
            Pandera -.->|Schema Breach| DLQ
        end

        subgraph ZONE_3 ["ZONE 3: SILVER COMPUTE & PROCESSING"]
            direction TB
            Spark["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/apachespark/apachespark-original.svg' width='30'/><br/><b style='color:#EA580C;'>EMR Serverless Spark</b><br/><span style='color:#64748B;'>AQE Skew-Join Processing</span>"]:::card_spark
            Silver["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='30'/><br/><b style='color:#D97706;'>Amazon S3 Silver Lake</b><br/><span style='color:#64748B;'>Cleaned & Typed Parquet</span>"]:::card_s3

            Spark --> Silver
        end

        subgraph ZONE_4 ["ZONE 4: GOLD LAKE & GOVERNANCE"]
            direction TB
            dbt["🟧<br/><b style='color:#EF4444;'>dbt-Athena Core 1.8</b><br/><span style='color:#64748B;'>42 Quality Assertions</span>"]:::card_dbt
            Gold["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='30'/><br/><b style='color:#D97706;'>Amazon S3 Gold Lake</b><br/><span style='color:#64748B;'>Enriched Player Marts</span>"]:::card_s3
            Glue["🗂️<br/><b style='color:#059669;'>AWS Glue Data Catalog</b><br/><span style='color:#64748B;'>Centralized Metastore</span>"]:::card_glue

            dbt --> Gold
            Glue -.- Silver
            Glue -.- Gold
        end

        subgraph ZONE_5 ["ZONE 5: SERVING & ANALYTICS"]
            direction TB
            Athena["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg' width='30'/><br/><b style='color:#0284C7;'>Amazon Athena SQL</b><br/><span style='color:#64748B;'>Serverless Query Engine</span>"]:::card_athena
            Redis["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original.svg' width='30'/><br/><b style='color:#DC2626;'>ElastiCache Redis</b><br/><span style='color:#64748B;'>Sub-10ms Hot Query Cache</span>"]:::card_redis
            FastAPI["<img src='https://raw.githubusercontent.com/devicons/devicon/master/icons/fastapi/fastapi-original.svg' width='30'/><br/><b style='color:#10B981;'>FastAPI on ECS</b><br/><span style='color:#64748B;'>Stateless REST API</span>"]:::card_fastapi

            Athena --> Redis
            Redis --> FastAPI
        end

        subgraph OPS_PLANE ["⚙️ CROSS-CUTTING OPERATIONAL PLANE (ORCHESTRATION, SECURITY & OBSERVABILITY)"]
            direction LR
            MWAA["🌀 <b>AWS MWAA Airflow</b><br/><span style='color:#64748B;'>Managed DAG Orchestrator</span>"]:::card_ops
            SEC["🔒 <b>AWS Secrets Manager & KMS</b><br/><span style='color:#64748B;'>IAM Roles & SSE-KMS Encryption</span>"]:::card_ops
            OBS["📊 <b>Amazon CloudWatch & X-Ray</b><br/><span style='color:#64748B;'>APM Tracing & SNS Slack Alerts</span>"]:::card_ops
        end

        Ingest ==>|1. Raw Parquet| Bronze
        Pandera ==>|2. Validated Data| Spark
        Silver ==>|3. Cleaned Models| dbt
        Gold ==>|4. Gold Scans| Athena

        MWAA .-> Ingest
        MWAA .-> Pandera
        MWAA .-> Spark
        MWAA .-> dbt
    end
"""

# Base64 encoding for URL parameters
encoded = base64.b64encode(mermaid_code.encode("utf-8")).decode("utf-8")
url_svg = f"https://mermaid.ink/svg/{encoded}?theme=neutral"
url_png = f"https://mermaid.ink/img/{encoded}?theme=neutral"

req_svg = urllib.request.Request(url_svg, headers={"User-Agent": "Mozilla/5.0"})
req_png = urllib.request.Request(url_png, headers={"User-Agent": "Mozilla/5.0"})

try:
    with urllib.request.urlopen(req_svg) as resp:
        svg_bytes = resp.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v4.svg", "wb"
        ) as f:
            f.write(svg_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_architecture.svg",
            "wb",
        ) as f:
            f.write(svg_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v3.svg", "wb"
        ) as f:
            f.write(svg_bytes)
    print("v4 Remediated SVG saved successfully!")
except Exception as e:
    print("SVG Error:", e)

try:
    with urllib.request.urlopen(req_png) as resp:
        png_bytes = resp.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v4.png", "wb"
        ) as f:
            f.write(png_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_architecture_diagram.png",
            "wb",
        ) as f:
            f.write(png_bytes)
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v3.png", "wb"
        ) as f:
            f.write(png_bytes)
    print("v4 Remediated PNG saved successfully!")
except Exception as e:
    print("PNG Error:", e)
