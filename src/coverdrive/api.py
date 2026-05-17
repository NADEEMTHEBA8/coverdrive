"""FastAPI service exposing the Gold marts as a versioned REST API.

The API reads from DuckDB — the same warehouse dbt populates. Read-only.
Pydantic models double as the OpenAPI schema, so `/docs` is auto-generated
and stays in sync with the response shape.

Endpoints follow REST conventions:
  GET /healthz                          — liveness
  GET /readyz                           — checks warehouse is reachable + has data
  GET /api/v1/players?limit=&offset=    — paginated player dimension
  GET /api/v1/rankings/batsmen?limit=   — top batsmen by PCA score
  GET /api/v1/rankings/bowlers?limit=   — top bowlers by PCA score
  GET /api/v1/players/{player}/stats    — career stats for one player
"""

from __future__ import annotations

import sys
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Annotated

import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from coverdrive.utils import configure_logging, get_logger, get_settings

log = get_logger(__name__)


# ─── Response models (also the OpenAPI schema) ────────────────────────────────


class HealthResponse(BaseModel):
    status: str
    version: str = "0.1.0"


class ReadyResponse(BaseModel):
    status: str
    warehouse_reachable: bool
    batsmen_rows: int
    bowlers_rows: int


class PlayerStats(BaseModel):
    player: str
    country_tag: str | None
    matches: int | None
    innings: int | None
    runs: int | None = None
    wickets: int | None = None
    pca_score: float | None = None


class RankingEntry(BaseModel):
    rank: int
    player: str
    country_tag: str | None
    pca_score: float
    matches: int | None
    primary_metric: float | None = Field(
        default=None, description="Runs for batsmen, wickets for bowlers"
    )


class PaginatedPlayers(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[PlayerStats]


# ─── DuckDB connection ────────────────────────────────────────────────────────
# DuckDB connections are cheap but not thread-safe. Use a short-lived
# read-only connection per request; FastAPI runs handlers concurrently.


@contextmanager
def warehouse_conn() -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield a read-only DuckDB connection. Closes deterministically."""
    settings = get_settings()
    conn = duckdb.connect(settings.coverdrive_warehouse_path, read_only=True)
    try:
        yield conn
    finally:
        conn.close()


# ─── Lifespan: bind config, ping warehouse on startup ─────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    log.info("api.startup", warehouse=get_settings().coverdrive_warehouse_path)
    yield
    log.info("api.shutdown")


app = FastAPI(
    title="Coverdrive API",
    description="Read-only access to cricket player performance marts.",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)


# ─── Middleware: request logging + timing ─────────────────────────────────────


@app.middleware("http")
async def log_requests(request: Request, call_next):  # type: ignore[no-untyped-def]  # noqa: ANN001, ANN201
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    log.info(
        "http.request",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=round(duration_ms, 2),
    )
    return response


# ─── Exception handlers ───────────────────────────────────────────────────────


@app.exception_handler(duckdb.Error)
async def handle_duckdb_error(_: Request, exc: duckdb.Error) -> JSONResponse:
    log.exception("api.warehouse_error")
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "warehouse unavailable", "error": str(exc)},
    )


# ─── Endpoints ────────────────────────────────────────────────────────────────


@app.get("/healthz", response_model=HealthResponse, tags=["health"])
def healthz() -> HealthResponse:
    """Liveness — process is up. No I/O."""
    return HealthResponse(status="ok")


@app.get("/readyz", response_model=ReadyResponse, tags=["health"])
def readyz() -> ReadyResponse:
    """Readiness — warehouse is reachable and contains expected marts."""
    try:
        with warehouse_conn() as conn:
            batsmen = conn.execute("SELECT COUNT(*) FROM main_marts.mart_top_batsmen").fetchone()
            bowlers = conn.execute("SELECT COUNT(*) FROM main_marts.mart_top_bowlers").fetchone()
        return ReadyResponse(
            status="ok",
            warehouse_reachable=True,
            batsmen_rows=int(batsmen[0]) if batsmen else 0,
            bowlers_rows=int(bowlers[0]) if bowlers else 0,
        )
    except Exception as e:
        log.warning("api.readyz_failed", error=str(e))
        return ReadyResponse(
            status="degraded", warehouse_reachable=False, batsmen_rows=0, bowlers_rows=0
        )


@app.get("/api/v1/rankings/batsmen", response_model=list[RankingEntry], tags=["rankings"])
def top_batsmen(
    limit: Annotated[int, Query(ge=1, le=200)] = 25,
) -> list[RankingEntry]:
    """Top batsmen by PCA composite score, descending."""
    with warehouse_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                ROW_NUMBER() OVER (ORDER BY pca_score DESC) AS rank,
                player, country_tag, pca_score, matches, runs AS primary_metric
            FROM main_marts.mart_top_batsmen
            ORDER BY pca_score DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    cols = ["rank", "player", "country_tag", "pca_score", "matches", "primary_metric"]
    return [RankingEntry(**dict(zip(cols, row, strict=True))) for row in rows]


@app.get("/api/v1/rankings/bowlers", response_model=list[RankingEntry], tags=["rankings"])
def top_bowlers(
    limit: Annotated[int, Query(ge=1, le=200)] = 25,
) -> list[RankingEntry]:
    """Top bowlers by PCA composite score, descending."""
    with warehouse_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                ROW_NUMBER() OVER (ORDER BY pca_score DESC) AS rank,
                player, country_tag, pca_score, matches, wickets AS primary_metric
            FROM main_marts.mart_top_bowlers
            ORDER BY pca_score DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    cols = ["rank", "player", "country_tag", "pca_score", "matches", "primary_metric"]
    return [RankingEntry(**dict(zip(cols, row, strict=True))) for row in rows]


@app.get("/api/v1/players/{player}/stats", response_model=PlayerStats, tags=["players"])
def player_stats(player: str) -> PlayerStats:
    """Career stats for a single player (lowercased name)."""
    with warehouse_conn() as conn:
        row = conn.execute(
            """
            SELECT player, country_tag, matches, innings, runs, wickets, pca_score
            FROM main_marts.dim_player
            WHERE LOWER(player) = LOWER(?)
            """,
            [player],
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Player not found: {player}")
    return PlayerStats(
        player=row[0],
        country_tag=row[1],
        matches=row[2],
        innings=row[3],
        runs=row[4],
        wickets=row[5],
        pca_score=row[6],
    )


# ─── Entrypoint ───────────────────────────────────────────────────────────────


def main() -> int:
    """Run with `python -m coverdrive.api`."""
    settings = get_settings()
    uvicorn.run(
        "coverdrive.api:app",
        host=settings.api_host,
        port=settings.api_port,
        log_config=None,  # We provide our own structured logging.
        reload=settings.coverdrive_env == "local",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
