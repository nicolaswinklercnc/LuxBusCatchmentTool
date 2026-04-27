"""Cache for discovered download URLs.

Stored at ingest/data/discovered_urls.json. On the next run we HEAD-check the
cached URL — if it still returns 200, we skip rediscovery.

Bypassed entirely when INGEST_FORCE_REFRESH=1 (set by `run_all.py --force-refresh`).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import requests

DATA_DIR = Path(__file__).resolve().parent / "data"
CACHE_FILE = DATA_DIR / "discovered_urls.json"


def _force_refresh() -> bool:
    return os.getenv("INGEST_FORCE_REFRESH") == "1"


def _load() -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _save(data: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(
        json.dumps(data, indent=2, sort_keys=True), encoding="utf-8"
    )


def head_ok(url: str, timeout: int = 15) -> bool:
    """Return True iff a HEAD (or fallback Range GET) confirms the URL is alive."""
    try:
        resp = requests.head(url, allow_redirects=True, timeout=timeout)
        if resp.status_code == 200:
            return True
        # Some CDNs disallow HEAD — probe with a 1-byte ranged GET instead
        if resp.status_code in (403, 405):
            r2 = requests.get(
                url, headers={"Range": "bytes=0-0"}, stream=True, timeout=timeout
            )
            r2.close()
            return r2.status_code in (200, 206)
    except requests.RequestException:
        return False
    return False


def get_cached_url(key: str) -> Optional[str]:
    """Return the cached URL for `key` if HEAD confirms it is still 200; else None."""
    if _force_refresh():
        return None
    cached = _load().get(key)
    if not cached:
        return None
    return cached if head_ok(cached) else None


def remember_url(key: str, url: str) -> None:
    data = _load()
    data[key] = url
    _save(data)
