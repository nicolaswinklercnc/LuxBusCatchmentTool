"""Shared database connection helpers for ingest scripts.

Reads DATABASE_URL from .env (loaded via python-dotenv). Exposes:
- get_engine(): SQLAlchemy engine, used by geopandas .to_postgis()
- get_psycopg2_conn(): raw psycopg2 connection, for explicit DDL / SELECT
- PROJECTED_CRS: the metric CRS all spatial ops use (default EPSG:3035)
"""
from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env and edit it, "
            "or export DATABASE_URL=postgresql://user:pass@host:port/dbname"
        )
    return url


def get_engine() -> Engine:
    return create_engine(_database_url(), future=True)


def get_psycopg2_conn():
    return psycopg2.connect(_database_url())


PROJECTED_CRS = os.getenv("PROJECTED_CRS", "EPSG:3035")
