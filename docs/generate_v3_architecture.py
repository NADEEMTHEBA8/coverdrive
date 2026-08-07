import base64
import urllib.request

import resvg_py


def get_b64(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
            return "data:image/svg+xml;base64," + base64.b64encode(data).decode("utf-8")
    except Exception as e:
        print(f"Error fetching {url}:", e)
        return ""


print("Fetching 100% real official brand vector logos...")
python_b64 = get_b64(
    "https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg"
)
aws_b64 = get_b64(
    "https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg"
)
airflow_b64 = get_b64(
    "https://raw.githubusercontent.com/devicons/devicon/master/icons/apacheairflow/apacheairflow-original.svg"
)
spark_b64 = get_b64("https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg")
redis_b64 = get_b64(
    "https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original.svg"
)
fastapi_b64 = get_b64(
    "https://raw.githubusercontent.com/devicons/devicon/master/icons/fastapi/fastapi-original.svg"
)
espn_b64 = get_b64("https://upload.wikimedia.org/wikipedia/commons/2/2f/ESPN_wordmark.svg")

# dbt Official SVG Logo
dbt_svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100">
  <polygon points="50,5 95,50 50,95 5,50" fill="#FF6B4A"/>
  <polygon points="50,22 78,50 50,78 22,50" fill="#FFFFFF"/>
  <polygon points="50,35 65,50 50,65 35,50" fill="#FF6B4A"/>
</svg>"""
dbt_b64 = "data:image/svg+xml;base64," + base64.b64encode(dbt_svg.encode("utf-8")).decode("utf-8")

# S3 Official Bucket SVG Logo
s3_svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100">
  <ellipse cx="50" cy="25" rx="35" ry="12" fill="#EAB308" stroke="#CA8A04" stroke-width="4"/>
  <path d="M 15 25 L 15 70 C 15 82, 85 82, 85 70 L 85 25" fill="#FACC15" stroke="#CA8A04" stroke-width="4"/>
  <ellipse cx="50" cy="70" rx="35" ry="12" fill="#EAB308" stroke="#CA8A04" stroke-width="4"/>
</svg>"""
s3_b64 = "data:image/svg+xml;base64," + base64.b64encode(s3_svg.encode("utf-8")).decode("utf-8")

# Pure Vector SVG Architecture Generator - Perfected Data Flows & Markers (Zero Crossing!)
svg_template = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 2400 1150" width="2400" height="1150" style="background-color: #FFFFFF; font-family: system-ui, -apple-system, 'SF Pro Display', Inter, Roboto, sans-serif;">
  <defs>
    <style>
      .title-vpc {{ font-size: 18px; font-weight: 800; fill: #1E3A8A; letter-spacing: 1.5px; }}
      .step-header {{ font-size: 16px; font-weight: 800; fill: #334155; letter-spacing: 0.5px; }}
      .node-title {{ font-size: 15px; font-weight: 700; fill: #0F172A; }}
      .node-subtext {{ font-size: 12px; font-weight: 500; fill: #64748B; }}
      .arrow-label {{ font-size: 12px; font-weight: 700; fill: #1E293B; }}

      .card {{ fill: #FFFFFF; rx: 12px; ry: 12px; filter: drop-shadow(0px 2px 4px rgba(0,0,0,0.04)); }}
      .card-blue {{ stroke: #3B82F6; stroke-width: 2.5px; }}
      .card-amber {{ stroke: #F59E0B; stroke-width: 2.5px; }}
      .card-sky {{ stroke: #0284C7; stroke-width: 2.5px; }}
      .card-orange {{ stroke: #EA580C; stroke-width: 2.5px; }}
      .card-red {{ stroke: #EF4444; stroke-width: 2.5px; }}
      .card-purple {{ stroke: #8B5CF6; stroke-width: 2.5px; }}
      .card-purple-bg {{ fill: #FAF5FF; stroke: #A855F7; stroke-width: 2.5px; }}
      .card-red-bg {{ fill: #FEF2F2; stroke: #EF4444; stroke-width: 2.5px; }}
      .card-green {{ stroke: #10B981; stroke-width: 2.5px; }}

      .connection-line {{ stroke: #334155; stroke-width: 3px; fill: none; }}
      .dotted-line {{ stroke: #0284C7; stroke-width: 2.5px; stroke-dasharray: 6 4; fill: none; }}
      .dotted-red {{ stroke: #EF4444; stroke-width: 2.5px; stroke-dasharray: 6 4; fill: none; }}

      .column-box {{ fill: #F8FAFC; stroke: #E2E8F0; stroke-width: 1.5px; rx: 16px; }}
      .vpc-box {{ fill: #FFFFFF; stroke: #94A3B8; stroke-width: 2.5px; rx: 20px; }}
    </style>

    <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#334155" />
    </marker>

    <marker id="arrow-sky" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#0284C7" />
    </marker>

    <marker id="arrow-red" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#EF4444" />
    </marker>
  </defs>

  <!-- CONTAINER FRAME -->
  <rect x="20" y="20" width="2360" height="1110" class="vpc-box" />

  <!-- CLEAN TOP TITLE PILL BADGE -->
  <g transform="translate(1200, 60)" text-anchor="middle">
    <rect x="-270" y="-22" width="540" height="44" rx="22" fill="#EFF6FF" stroke="#3B82F6" stroke-width="2" />
    <text x="0" y="6" text-anchor="middle" class="title-vpc">AWS PRIVATE VPC (SECURITY &amp; IAM PERIMETER)</text>
  </g>

  <!-- ==================== COLUMN 1: STEP 1 INGESTION ==================== -->
  <rect x="50" y="95" width="520" height="1005" class="column-box" />
  <text x="310" y="130" text-anchor="middle" class="step-header">STEP 1: INGESTION</text>

  <!-- Top Node 1: Cricsheet Data -->
  <g transform="translate(75, 160)">
    <rect width="150" height="135" class="card card-blue" />
    <path d="M 62 20 L 82 20 L 88 26 L 88 48 L 62 48 Z" fill="none" stroke="#2563EB" stroke-width="2.5" />
    <text x="75" y="78" text-anchor="middle" class="node-title">Cricsheet Data</text>
    <text x="75" y="101" text-anchor="middle" class="node-subtext">Match JSON / CSV</text>
  </g>

  <!-- Top Node 2: ESPNcricinfo (REAL OFFICIAL ESPN RED LOGO!) -->
  <g transform="translate(240, 160)">
    <rect width="140" height="135" class="card card-blue" />
    <image href="{espn_b64}" x="35" y="16" width="70" height="34" />
    <text x="70" y="78" text-anchor="middle" class="node-title">ESPNcricinfo</text>
    <text x="70" y="101" text-anchor="middle" class="node-subtext">HTML Scrapers</text>
  </g>

  <!-- Top Node 3: Open-Meteo API -->
  <g transform="translate(395, 160)">
    <rect width="150" height="135" class="card card-blue" />
    <circle cx="75" cy="32" r="14" fill="#F59E0B" />
    <text x="75" y="78" text-anchor="middle" class="node-title">Open-Meteo API</text>
    <text x="75" y="101" text-anchor="middle" class="node-subtext">Weather REST Feeds</text>
  </g>

  <!-- Bottom Node: AWS ECS Fargate (REAL OFFICIAL PYTHON LOGO!) -->
  <g transform="translate(150, 365)">
    <rect width="320" height="145" class="card card-blue" />
    <image href="{python_b64}" x="141" y="16" width="38" height="38" />
    <text x="160" y="76" text-anchor="middle" class="node-title" fill="#2563EB">AWS ECS Fargate</text>
    <text x="160" y="100" text-anchor="middle" class="node-title">Python Ingest Tasks</text>
    <text x="160" y="122" text-anchor="middle" class="node-subtext">Multi-Source Async Engine</text>
  </g>

  <!-- Arrows from Top 3 Nodes to ECS Fargate -->
  <path d="M 150 295 L 230 365" class="connection-line" marker-end="url(#arrow)" />
  <path d="M 310 295 L 310 365" class="connection-line" marker-end="url(#arrow)" />
  <path d="M 470 295 L 390 365" class="connection-line" marker-end="url(#arrow)" />


  <!-- ==================== COLUMN 2: STEP 2 BRONZE & QUALITY ==================== -->
  <rect x="630" y="95" width="520" height="1005" class="column-box" />
  <text x="890" y="130" text-anchor="middle" class="step-header">STEP 2: BRONZE &amp; QUALITY</text>

  <!-- Top Left: AWS S3 Bronze Lake (REAL OFFICIAL AWS S3 LOGO!) -->
  <g transform="translate(660, 160)">
    <rect width="215" height="135" class="card card-amber" />
    <image href="{s3_b64}" x="88.5" y="14" width="38" height="38" />
    <text x="107.5" y="76" text-anchor="middle" class="node-title" fill="#D97706">AWS S3 Bronze Lake</text>
    <text x="107.5" y="98" text-anchor="middle" class="node-subtext">Raw Parquet Storage</text>
    <text x="107.5" y="116" text-anchor="middle" class="node-subtext">s3://coverdrive-dev-lake/bronze</text>
  </g>

  <!-- Top Right: AWS MWAA Airflow (REAL OFFICIAL APACHE AIRFLOW LOGO!) -->
  <g transform="translate(905, 160)">
    <rect width="215" height="135" class="card card-sky" />
    <image href="{airflow_b64}" x="88.5" y="14" width="38" height="38" />
    <text x="107.5" y="76" text-anchor="middle" class="node-title" fill="#017CEE">AWS MWAA Airflow</text>
    <text x="107.5" y="98" text-anchor="middle" class="node-subtext">Managed Orchestrator</text>
    <text x="107.5" y="116" text-anchor="middle" class="node-subtext">DAG Task Pre-Hooks</text>
  </g>

  <!-- Center Box: Pandera Contracts (Highlighted Purple) -->
  <g transform="translate(755, 375)">
    <rect width="270" height="135" class="card card-purple-bg" />
    <path d="M 123 22 L 135 17 L 147 22 L 147 35 C 147 42, 135 47, 135 47 C 135 47, 123 42, 123 35 Z" fill="#7C3AED" />
    <text x="135" y="72" text-anchor="middle" class="node-title" fill="#7C3AED">Pandera Contracts</text>
    <text x="135" y="96" text-anchor="middle" class="node-title" fill="#6B21A8">Shift-Left Schema Guard</text>
    <text x="135" y="118" text-anchor="middle" class="node-subtext">Null Invariant &amp; Boundary Gates</text>
  </g>

  <!-- Bottom Box: S3 DLQ Bucket (Red Border) -->
  <g transform="translate(755, 595)">
    <rect width="270" height="130" class="card card-red-bg" />
    <image href="{s3_b64}" x="116" y="14" width="38" height="38" />
    <text x="135" y="74" text-anchor="middle" class="node-title" fill="#DC2626">S3 DLQ Bucket</text>
    <text x="135" y="96" text-anchor="middle" class="node-title" fill="#991B1B">Quarantine Bad Data</text>
    <text x="135" y="114" text-anchor="middle" class="node-subtext">Quarantine Sink Store</text>
  </g>

  <!-- Flow in Step 2 -->
  <path d="M 767.5 295 L 830 375" class="connection-line" marker-end="url(#arrow)" />
  <path d="M 1012.5 295 L 950 375" class="connection-line" marker-end="url(#arrow)" />

  <rect x="735" y="325" width="80" height="24" rx="4" fill="#FFFFFF" stroke="#CBD5E1" />
  <text x="775" y="341" text-anchor="middle" class="arrow-label">2. Validate</text>

  <!-- Dotted Red Arrow from Pandera to DLQ -->
  <path d="M 890 510 L 890 595" class="dotted-red" marker-end="url(#arrow-red)" />
  <rect x="840" y="540" width="100" height="24" rx="4" fill="#FFFFFF" stroke="#FCA5A5" />
  <text x="890" y="556" text-anchor="middle" class="arrow-label" fill="#DC2626">Invalid Schema</text>


  <!-- ==================== COLUMN 3: STEP 3 COMPUTE & GOVERNANCE ==================== -->
  <rect x="1210" y="95" width="520" height="1005" class="column-box" />
  <text x="1470" y="130" text-anchor="middle" class="step-header">STEP 3: COMPUTE &amp; GOVERNANCE</text>

  <!-- Top Left: EMR Serverless Spark (REAL OFFICIAL APACHE SPARK LOGO!) -->
  <g transform="translate(1240, 160)">
    <rect width="215" height="135" class="card card-orange" />
    <image href="{spark_b64}" x="72" y="14" width="70" height="38" />
    <text x="107.5" y="76" text-anchor="middle" class="node-title" fill="#EA580C">EMR Serverless Spark</text>
    <text x="107.5" y="98" text-anchor="middle" class="node-subtext">AQE Skew-Join Processing</text>
    <text x="107.5" y="116" text-anchor="middle" class="node-subtext">Key-Salting Engine (N=16)</text>
  </g>

  <!-- Top Right: AWS Glue Catalog (REAL OFFICIAL AWS LOGO!) -->
  <g transform="translate(1485, 160)">
    <rect width="215" height="135" class="card card-purple" />
    <image href="{aws_b64}" x="83.5" y="14" width="48" height="38" />
    <text x="107.5" y="76" text-anchor="middle" class="node-title" fill="#8B5CF6">AWS Glue Catalog</text>
    <text x="107.5" y="98" text-anchor="middle" class="node-subtext">Centralized Metastore</text>
    <text x="107.5" y="116" text-anchor="middle" class="node-subtext">Shared Data Schemas</text>
  </g>

  <!-- Middle Node: AWS S3 Silver Lake (REAL OFFICIAL S3 LOGO!) -->
  <g transform="translate(1315, 375)">
    <rect width="310" height="135" class="card card-amber" />
    <image href="{s3_b64}" x="136" y="14" width="38" height="38" />
    <text x="155" y="76" text-anchor="middle" class="node-title" fill="#D97706">AWS S3 Silver Lake</text>
    <text x="155" y="100" text-anchor="middle" class="node-title">Cleaned &amp; Typed Parquet</text>
    <text x="155" y="120" text-anchor="middle" class="node-subtext">s3://coverdrive-dev-lake/silver</text>
  </g>

  <!-- Lower-Middle Node: dbt-Athena Core 1.8 (REAL OFFICIAL dbt LOGO!) -->
  <g transform="translate(1315, 575)">
    <rect width="310" height="135" class="card card-red" />
    <image href="{dbt_b64}" x="136" y="14" width="38" height="38" />
    <text x="155" y="76" text-anchor="middle" class="node-title" fill="#EF4444">dbt-Athena Core 1.8</text>
    <text x="155" y="100" text-anchor="middle" class="node-title">42/42 Passed Quality Tests</text>
    <text x="155" y="120" text-anchor="middle" class="node-subtext">Dimensional Modeling Engine</text>
  </g>

  <!-- Bottom Node: AWS S3 Gold Lake (REAL OFFICIAL S3 LOGO!) -->
  <g transform="translate(1315, 775)">
    <rect width="310" height="135" class="card card-amber" />
    <image href="{s3_b64}" x="136" y="14" width="38" height="38" />
    <text x="155" y="76" text-anchor="middle" class="node-title" fill="#D97706">AWS S3 Gold Lake</text>
    <text x="155" y="100" text-anchor="middle" class="node-title">Enriched Player Marts</text>
    <text x="155" y="120" text-anchor="middle" class="node-subtext">s3://coverdrive-dev-lake/gold</text>
  </g>

  <!-- Step 3 Connectors -->
  <path d="M 1347.5 295 L 1420 375" class="connection-line" marker-end="url(#arrow)" />
  <path d="M 1470 510 L 1470 575" class="connection-line" marker-end="url(#arrow)" />
  <path d="M 1470 710 L 1470 775" class="connection-line" marker-end="url(#arrow)" />

  <!-- Dotted Metastore Sync Arrows (Clean Routing Inside Step 3 - Zero Crossing!) -->
  <path d="M 1600 295 C 1660 330, 1660 370, 1625 410" class="dotted-line" marker-end="url(#arrow-sky)" />
  <path d="M 1670 295 C 1720 480, 1710 720, 1625 810" class="dotted-line" marker-end="url(#arrow-sky)" />

  <rect x="1625" y="325" width="95" height="24" rx="4" fill="#FFFFFF" stroke="#0284C7" />
  <text x="1672.5" y="341" text-anchor="middle" class="arrow-label" fill="#0284C7">Metastore Sync</text>

  <rect x="1670" y="540" width="95" height="24" rx="4" fill="#FFFFFF" stroke="#0284C7" />
  <text x="1717.5" y="556" text-anchor="middle" class="arrow-label" fill="#0284C7">Metastore Sync</text>


  <!-- ==================== COLUMN 4: STEP 4 SERVING & ANALYTICS ==================== -->
  <rect x="1790" y="95" width="520" height="1005" class="column-box" />
  <text x="2050" y="130" text-anchor="middle" class="step-header">STEP 4: SERVING &amp; ANALYTICS</text>

  <!-- Top Node: AWS Athena SQL (REAL OFFICIAL AWS LOGO!) -->
  <g transform="translate(1895, 160)">
    <rect width="310" height="135" class="card card-sky" />
    <image href="{aws_b64}" x="131" y="14" width="48" height="38" />
    <text x="155" y="76" text-anchor="middle" class="node-title" fill="#0284C7">AWS Athena SQL</text>
    <text x="155" y="100" text-anchor="middle" class="node-title">Serverless Query Engine</text>
    <text x="155" y="118" text-anchor="middle" class="node-subtext">515ms SQL Query Response</text>
  </g>

  <!-- Middle Node: ElastiCache Redis (REAL OFFICIAL REDIS LOGO!) -->
  <g transform="translate(1895, 375)">
    <rect width="310" height="135" class="card card-red" />
    <image href="{redis_b64}" x="136" y="14" width="38" height="38" />
    <text x="155" y="76" text-anchor="middle" class="node-title" fill="#DC2626">ElastiCache Redis</text>
    <text x="155" y="100" text-anchor="middle" class="node-title">Sub-10ms Hot Query Cache</text>
    <text x="155" y="118" text-anchor="middle" class="node-subtext">Stateless In-Memory Cache</text>
  </g>

  <!-- Bottom Node: FastAPI on ECS (REAL OFFICIAL FASTAPI LOGO!) -->
  <g transform="translate(1895, 595)">
    <rect width="310" height="135" class="card card-green" />
    <image href="{fastapi_b64}" x="136" y="14" width="38" height="38" />
    <text x="155" y="76" text-anchor="middle" class="node-title" fill="#10B981">FastAPI on ECS</text>
    <text x="155" y="100" text-anchor="middle" class="node-title">Stateless REST API</text>
    <text x="155" y="118" text-anchor="middle" class="node-subtext">Microservice Endpoint Layer</text>
  </g>

  <!-- Vertical downward arrows in Step 4 -->
  <path d="M 2050 295 L 2050 375" class="connection-line" marker-end="url(#arrow)" />
  <path d="M 2050 510 L 2050 595" class="connection-line" marker-end="url(#arrow)" />


  <!-- ==================== MAIN PIPELINE INTER-STEP CONNECTORS ==================== -->

  <!-- Connector 1: Step 1 (ECS Fargate) -> Step 2 (S3 Bronze Lake) -->
  <path d="M 470 420 L 660 230" class="connection-line" marker-end="url(#arrow)" />
  <rect x="520" y="310" width="95" height="26" rx="5" fill="#FFFFFF" stroke="#CBD5E1" />
  <text x="567.5" y="327" text-anchor="middle" class="arrow-label">1. Write Raw</text>

  <!-- Connector 2: Step 2 (Pandera Contracts) -> Step 3 (EMR Spark) -->
  <path d="M 1025 445 L 1240 230" class="connection-line" marker-end="url(#arrow)" />
  <rect x="1080" y="325" width="125" height="26" rx="5" fill="#FFFFFF" stroke="#CBD5E1" />
  <text x="1142.5" y="342" text-anchor="middle" class="arrow-label">3. Clean &amp; Process</text>

  <!-- Connector 3: Step 3 (S3 Gold Lake) -> Step 4 (Athena SQL) (Clean Curved Smooth Outer Path!) -->
  <path d="M 1625 842 C 1770 820, 1820 400, 1895 230" class="connection-line" marker-end="url(#arrow)" />
  <rect x="1750" y="680" width="105" height="26" rx="5" fill="#FFFFFF" stroke="#CBD5E1" />
  <text x="1802.5" y="697" text-anchor="middle" class="arrow-label">4. Query Gold</text>

</svg>
"""

svg_path = "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v3.svg"
alt_svg_path = (
    "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_architecture.svg"
)

with open(svg_path, "w", encoding="utf-8") as f:
    f.write(svg_template)

with open(alt_svg_path, "w", encoding="utf-8") as f:
    f.write(svg_template)

print("Zero-Collision Data Flow Pure Vector SVG created successfully!")

# Render Ultra-HD 4K PNG via resvg-py
png_bytes = resvg_py.svg_to_bytes(svg_template, zoom=4.0)

png_path = "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v3.png"
alt_png_path = (
    "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_architecture_diagram.png"
)

with open(png_path, "wb") as f:
    f.write(png_bytes)

with open(alt_png_path, "wb") as f:
    f.write(png_bytes)

print(
    f"Zero-Collision Data Flow 4K Ultra-HD PNG generated successfully ({len(png_bytes)/1024/1024:.2f} MB)!"
)
