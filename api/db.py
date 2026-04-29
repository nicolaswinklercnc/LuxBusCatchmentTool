"""Database helpers for the FastAPI app.

Provides a pooled SQLAlchemy engine and two query helpers used across the
endpoints:
  - run_query: execute a parameterised SQL statement and return rows as dicts.
  - run_geo_query: execute a SQL statement whose first column is a
    ST_AsGeoJSON string and the rest are properties; return a GeoJSON
    FeatureCollection as a Python dict.
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _select_env_file() -> Path:
    """Pick .env.local / .env.staging / .env.production based on ENVIRONMENT.

    Same selection rule as ingest/db.py — keeps ingest and the API in sync.
    """
    env = (os.getenv("ENVIRONMENT") or "").strip().lower()
    if env == "production":
        return PROJECT_ROOT / ".env.production"
    if env == "staging":
        return PROJECT_ROOT / ".env.staging"
    return PROJECT_ROOT / ".env.local"


def _load_env() -> None:
    """Load the chosen env file. No-ops silently if neither file exists AND
    DATABASE_URL is already in the process env (the case inside the API
    container, where docker-compose / Fly secrets supplies DATABASE_URL)."""
    chosen = _select_env_file()
    if chosen.exists():
        load_dotenv(chosen, override=False)
    elif not os.getenv("DATABASE_URL"):
        raise RuntimeError(
            f"No env file found at {chosen}.\n"
            f"  Local development: copy .env.example to .env.local.\n"
            f"  Staging:           set ENVIRONMENT=staging and create .env.staging.\n"
            f"  Production:        set ENVIRONMENT=production and create .env.production."
        )


_load_env()


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env.local and "
            "edit it, or set ENVIRONMENT=production with a populated "
            ".env.production."
        )
    return url


_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            _database_url(),
            future=True,
            pool_size=5,
            max_overflow=5,
            pool_pre_ping=True,
        )
    return _engine


@contextmanager
def get_connection() -> Iterator[Connection]:
    engine = get_engine()
    with engine.connect() as conn:
        yield conn


def run_query(sql: str, params: dict[str, Any] | None = None) -> list[dict]:
    with get_connection() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(row) for row in result.mappings()]


def run_geo_query(
    sql: str, params: dict[str, Any] | None = None, geom_col: str = "geom_json"
) -> dict:
    """Execute `sql` and assemble a GeoJSON FeatureCollection.

    The query must return one column named `geom_col` (a ST_AsGeoJSON string)
    plus any number of property columns. Rows where the geometry is NULL are
    skipped.
    """
    rows = run_query(sql, params)
    features = []
    for row in rows:
        geom_str = row.pop(geom_col, None)
        if geom_str is None:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": json.loads(geom_str),
                "properties": row,
            }
        )
    return {"type": "FeatureCollection", "features": features}
