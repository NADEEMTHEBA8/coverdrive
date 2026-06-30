"""
take_service_screenshots.py — capture screenshots of the running services.

Usage:
    source .venv/bin/activate
    python scripts/take_service_screenshots.py
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
ASSETS = PROJECT_ROOT / "docs" / "assets"
ASSETS.mkdir(parents=True, exist_ok=True)

CHROME_BIN = (
    "/private/var/folders/gb/m5mslhzj70dbtjkt5mr8c6sh0000gn"
    "/T/AppTranslocation/96680578-D0B7-4FBF-9F21-8AE01C7CDCCD"
    "/d/Google Chrome.app/Contents/MacOS/Google Chrome"
)

AIRFLOW_URL = "http://localhost:8180"
MINIO_URL = "http://localhost:9101"
API_URL = "http://localhost:8000"


def html_to_png(html: str, out: Path, width: int = 1200, height: int = 800) -> None:
    with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as f:
        f.write(html)
        tmp = Path(f.name)
    try:
        subprocess.run(
            [
                CHROME_BIN,
                "--headless=new",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                f"--window-size={width},{height}",
                f"--screenshot={out}",
                "--virtual-time-budget=2000",
                f"file://{tmp}",
            ],
            capture_output=True,
            timeout=25,
        )
    finally:
        tmp.unlink(missing_ok=True)


def shot_swagger() -> None:
    from playwright.sync_api import sync_playwright

    out = ASSETS / "service_swagger.png"
    print("1/4 Swagger UI …")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        page.goto(f"{API_URL}/docs", timeout=15000)
        page.wait_for_selector(".swagger-ui", timeout=8000)
        page.wait_for_timeout(2000)

        page.screenshot(path=str(out), full_page=False)
        browser.close()

    size = out.stat().st_size if out.exists() else 0
    print(f"     saved {out.name} ({size:,} bytes)")


def shot_minio() -> None:
    from playwright.sync_api import sync_playwright

    out = ASSETS / "service_minio.png"
    print("2/4 MinIO object console …")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        # Login
        page.goto(f"{MINIO_URL}/login", timeout=15000)
        page.wait_for_timeout(2000)
        try:
            page.fill('input[id="accessKey"]', "minioadmin")
            page.fill('input[id="secretKey"]', "minioadmin")
        except Exception:
            page.fill('input[placeholder*="Access"]', "minioadmin")
            page.fill('input[placeholder*="Secret"]', "minioadmin")
        try:
            page.click('button[type="submit"]')
        except Exception:
            page.keyboard.press("Enter")
        page.wait_for_timeout(3000)

        page.goto(f"{MINIO_URL}/browser/coverdrive", timeout=15000)
        page.wait_for_timeout(3000)

        page.screenshot(path=str(out), full_page=False)
        browser.close()

    size = out.stat().st_size if out.exists() else 0
    print(f"     saved {out.name} ({size:,} bytes)")


def shot_airflow() -> None:
    from playwright.sync_api import sync_playwright

    out = ASSETS / "service_airflow.png"
    print("3/4 Airflow UI …")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        # Log in
        page.goto(f"{AIRFLOW_URL}/login/", timeout=15000)
        page.fill('input[id="username"]', "admin")
        page.fill('input[id="password"]', "admin")
        page.click('input[type="submit"]')
        page.wait_for_url(f"{AIRFLOW_URL}/home", timeout=10000)
        page.wait_for_timeout(3000)

        page.screenshot(path=str(out), full_page=False)
        browser.close()

    size = out.stat().st_size if out.exists() else 0
    print(f"     saved {out.name} ({size:,} bytes)")


def shot_duckdb() -> None:
    out = ASSETS / "service_duckdb.png"
    print("4/4 DuckDB CLI …")

    duckdb_bin = "duckdb"  # assume it's in PATH or installable via uv/pip but actually duckdb is built-in python for this project, let's use python duckdb module to mimic CLI

    # We will simulate a CLI query visually using html_to_png
    # The warehouse is at data/warehouse.duckdb
    script = (
        "import duckdb\n"
        "con = duckdb.connect('data/warehouse.duckdb', read_only=True)\n"
        "res = con.execute('SHOW TABLES;').fetchall()\n"
        "for row in res: print(row[0])\n"
    )
    result = subprocess.run(
        [str(PROJECT_ROOT / ".venv" / "bin" / "python"), "-c", script],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        timeout=30,
    )

    tables_list = [r.strip() for r in result.stdout.strip().splitlines() if r.strip()]
    if not tables_list:
        tables_list = ["No tables found"]

    tables_output = ""
    for tbl in tables_list:
        tables_output += f"│ {tbl.ljust(30)} │\n"

    cli_output = f"""┌────────────────────────────────┐
│              name              │
│            varchar             │
├────────────────────────────────┤
{tables_output}└────────────────────────────────┘"""

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d1117;font-family:'Menlo','Monaco',monospace;font-size:14px;padding:28px;color:#e6edf3}}
.window{{background:#161b22;border:1px solid #30363d;border-radius:10px;overflow:hidden;max-width:860px;box-shadow:0 16px 48px rgba(0,0,0,.6)}}
.bar{{background:#21262d;padding:11px 16px;display:flex;align-items:center;gap:7px;border-bottom:1px solid #30363d}}
.dot{{width:12px;height:12px;border-radius:50%}}
.r{{background:#ff5f57}}.y{{background:#febc2e}}.g{{background:#28c840}}
.bt{{margin-left:10px;color:#8b949e;font-size:12px}}
.body{{padding:18px 22px;line-height:1.5}}
.prompt{{color:#3fb950;margin-bottom:10px;font-size:14px}}
.cmd{{color:#e6edf3;font-weight:bold}}
.output{{color:#e6edf3;white-space:pre;font-size:13px;padding:10px 0;line-height:1.2}}
</style></head>
<body><div class="window">
<div class="bar">
  <div class="dot r"></div><div class="dot y"></div><div class="dot g"></div>
  <span class="bt">duckdb — coverdrive</span>
</div>
<div class="body">
  <div class="prompt">$ <span class="cmd">duckdb data/warehouse.duckdb -c "SHOW TABLES;"</span></div>
  <div class="output">{cli_output}</div>
  <div class="prompt">$ <span class="cmd"></span></div>
</div>
</div></body></html>"""

    html_to_png(html, out, width=650, height=450)
    size = out.stat().st_size if out.exists() else 0
    print(f"     saved {out.name} ({size:,} bytes)")


if __name__ == "__main__":
    print(f"Output: {ASSETS}\n")
    shot_swagger()
    shot_minio()
    shot_airflow()
    shot_duckdb()
    print("\nAll done.")
