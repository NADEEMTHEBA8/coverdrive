import base64
import urllib.request

# v3 Enhanced Publication-Grade Architecture Diagram
mermaid_code = """
flowchart LR
    %% Executive ByteByteGo Palette & Card Design Systems
    classDef card_python fill:#FFFFFF,stroke:#3B82F6,stroke-width:2px,color:#0F172A;
    classDef card_s3 fill:#FFFFFF,stroke:#F59E0B,stroke-width:2px,color:#0F172A;
    classDef card_airflow fill:#FFFFFF,stroke:#0284C7,stroke-width:2px,color:#0F172A;
    classDef card_spark fill:#FFFFFF,stroke:#EA580C,stroke-width:2px,color:#0F172A;
    classDef card_dbt fill:#FFFFFF,stroke:#EF4444,stroke-width:2px,color:#0F172A;
    classDef card_athena fill:#FFFFFF,stroke:#0284C7,stroke-width:2px,color:#0F172A;
    classDef card_redis fill:#FFFFFF,stroke:#DC2626,stroke-width:2px,color:#0F172A;
    classDef card_fastapi fill:#FFFFFF,stroke:#10B981,stroke-width:2px,color:#0F172A;
    classDef card_pandera fill:#FFFFFF,stroke:#A855F7,stroke-width:2px,color:#0F172A;
    classDef card_glue fill:#FFFFFF,stroke:#059669,stroke-width:2px,color:#0F172A;
    classDef card_dlq fill:#FEF2F2,stroke:#EF4444,stroke-width:2px,color:#991B1B;

    subgraph VPC ["🛡️ AWS PRIVATE VPC (SECURITY & IAM PERIMETER)"]
        direction LR

        %% STAGE 1: INGESTION (All 3 Multi-Source Ingestion Feeds)
        subgraph S1 ["STEP 01: MULTI-SOURCE INGESTION"]
            direction TB
            Src1["📄 <b>Cricsheet Archives</b><br/><span style='color:#64748B;'>Ball-by-Ball JSON / CSV</span>"]:::card_python
            Src2["🌐 <b>ESPNcricinfo</b><br/><span style='color:#64748B;'>HTML Player Scrapers</span>"]:::card_python
            Src3["🌤️ <b>Open-Meteo API</b><br/><span style='color:#64748B;'>Historical Weather REST</span>"]:::card_python
            Ingest["🐍<br/><b style='color:#2563EB;'>AWS ECS Fargate</b><br/><span style='color:#64748B;'>Python Container Tasks</span>"]:::card_python
            Src1 --> Ingest
            Src2 --> Ingest
            Src3 --> Ingest
        end

        %% STAGE 2: BRONZE & QUALITY
        subgraph S2 ["STEP 02: BRONZE & QUALITY GATE"]
            direction TB
            Bronze["🪣<br/><b style='color:#D97706;'>AWS S3 Bronze Lake</b><br/><span style='color:#64748B;'>Raw Parquet Storage</span>"]:::card_s3
            Airflow["🌀<br/><b style='color:#017CEE;'>AWS MWAA Airflow</b><br/><span style='color:#64748B;'>Managed DAG Orchestrator</span>"]:::card_airflow
            Pandera["🛡️<br/><b style='color:#C026D3;'>Pandera Contracts</b><br/><span style='color:#64748B;'>Shift-Left Schema Guard</span>"]:::card_pandera
            DLQ["📥<br/><b style='color:#DC2626;'>S3 DLQ Bucket</b><br/><span style='color:#991B1B;'>Quarantine Sink</span>"]:::card_dlq

            Airflow --> Pandera
            Pandera -.->|Schema Breach| DLQ
        end

        %% STAGE 3: COMPUTE & GOVERNANCE
        subgraph S3 ["STEP 03: COMPUTE & GOVERNANCE"]
            direction TB
            Spark["⚡<br/><b style='color:#EA580C;'>EMR Serverless Spark</b><br/><span style='color:#64748B;'>AQE Skew-Join Processing</span>"]:::card_spark
            Glue["🗂️<br/><b style='color:#059669;'>AWS Glue Data Catalog</b><br/><span style='color:#64748B;'>Centralized Metastore</span>"]:::card_glue
            Silver["🪣<br/><b style='color:#D97706;'>AWS S3 Silver Lake</b><br/><span style='color:#64748B;'>Cleaned & Typed Parquet</span>"]:::card_s3
            dbt["🟧<br/><b style='color:#EF4444;'>dbt-Athena Core 1.8</b><br/><span style='color:#64748B;'>42 Passed Assertions</span>"]:::card_dbt
            Gold["🪣<br/><b style='color:#D97706;'>AWS S3 Gold Lake</b><br/><span style='color:#64748B;'>Enriched Player Marts</span>"]:::card_s3

            Spark --> Silver
            Silver --> dbt
            dbt --> Gold
            Glue -. Metastore Sync .-> Silver
            Glue -. Metastore Sync .-> Gold
        end

        %% STAGE 4: SERVING & ANALYTICS
        subgraph S4 ["STEP 04: SERVING & ANALYTICS"]
            direction TB
            Athena["☁️<br/><b style='color:#0284C7;'>AWS Athena SQL</b><br/><span style='color:#64748B;'>Serverless Query Engine</span>"]:::card_athena
            Redis["🔴<br/><b style='color:#DC2626;'>ElastiCache Redis</b><br/><span style='color:#64748B;'>Sub-10ms Hot Query Cache</span>"]:::card_redis
            FastAPI["⚡<br/><b style='color:#10B981;'>FastAPI on ECS</b><br/><span style='color:#64748B;'>Stateless REST API</span>"]:::card_fastapi

            Athena --> Redis
            Redis --> FastAPI
        end

        %% SEQUENTIAL DATA FLOW CONNECTIONS WITH ENHANCED DESCRIPTIVE BADGES
        Ingest -->|1. Write Raw Parquet| Bronze
        Bronze -->|2. Validate Schema| Pandera
        Pandera -->|3. Clean & Transform| Spark
        Gold -->|4. Query Gold Marts| Athena
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
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v3_enhanced.svg",
            "wb",
        ) as f:
            f.write(svg_bytes)
    print("v3 Enhanced SVG saved successfully!")
except Exception as e:
    print("SVG Error:", e)

try:
    with urllib.request.urlopen(req_png) as resp:
        png_bytes = resp.read()
        with open(
            "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v3_enhanced.png",
            "wb",
        ) as f:
            f.write(png_bytes)
    print("v3 Enhanced PNG saved successfully!")
except Exception as e:
    print("PNG Error:", e)
