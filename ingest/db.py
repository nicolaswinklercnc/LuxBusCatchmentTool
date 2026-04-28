"""Shared database connection helpers for ingest scripts.

Loads either `.env.local` or `.env.production` based on the ENVIRONMENT
variable in the shell, then reads DATABASE_URL from the resulting env.

  ENVIRONMENT unset or != "production"  -> .env.local      (local Docker)
  ENVIRONMENT == "production"           -> .env.production (Supabase)

This means `python ingest/run_all.py` always hits local Docker, and you
have to type `ENVIRONMENT=production python ingest/run_all.py` to touch
the live database. Production access is opt-in.

Exposes:
- get_engine(): SQLAlchemy engine, used by geopandas .to_postgis()
- get_psycopg2_conn(): raw psycopg2 connection, for explicit DDL / SELECT
- PROJECTED_CRS: the metric CRS all spatial ops use (default EPSG:3035)
- ENVIRONMENT: the resolved environment name (informational)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _select_env_file() -> Path:
    """Pick .env.local or .env.production based on ENVIRONMENT."""
    env = (os.getenv("ENVIRONMENT") or "").strip().lower()
    if env == "production":
        return PROJECT_ROOT / ".env.production"
    return PROJECT_ROOT / ".env.local"


def _load_env() -> str:
    """Load the chosen env file. Returns the resolved ENVIRONMENT name.

    Errors loudly if neither env file exists AND DATABASE_URL is not already
    in the process environment (the latter happens inside the API container,
    where DATABASE_URL is set by docker-compose / Fly secrets).
    """
    chosen = _select_env_file()
    if chosen.exists():
        load_dotenv(chosen, override=False)
    elif not os.getenv("DATABASE_URL"):
        local = PROJECT_ROOT / ".env.local"
        prod = PROJECT_ROOT / ".env.production"
        raise RuntimeError(
            f"No env file found at {chosen}.\n"
            f"  Local development: copy .env.example to .env.local and run again.\n"
            f"  Production:        set ENVIRONMENT=production and create .env.production\n"
            f"                     (see .env.example for the layout).\n"
            f"  Looked for: {local}, {prod}"
        )
    return (os.getenv("ENVIRONMENT") or "development").strip().lower()


ENVIRONMENT = _load_env()

# Surface to the user which DB this process will touch. Stays high-signal
# since the variable is now load-bearing for "am I about to write to prod?".
print(f"[db] ENVIRONMENT={ENVIRONMENT}", file=sys.stderr)


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env.local and "
            "edit it, or set ENVIRONMENT=production with a populated "
            ".env.production."
        )
    return url


def get_engine() -> Engine:
    return create_engine(_database_url(), future=True)


def get_psycopg2_conn():
    return psycopg2.connect(_database_url())


PROJECTED_CRS = os.getenv("PROJECTED_CRS", "EPSG:3035")
