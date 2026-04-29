"""Endpoint tests for the FastAPI app.

These hit a live PostGIS database via FastAPI's TestClient — integration
tests, not pure unit tests. They run in two contexts:

  - Local: against the developer's Docker DB after `python ingest/run_all.py`.
    The default constants below match the Eurostat / GTFS dataset.
  - CI: against a tiny hand-crafted fixture (`api/test_fixtures/init.sql`).
    The CI workflow exports env vars (EXPECTED_STOPS, TEST_STOP_ID, …) that
    override the defaults so the same assertions resolve to fixture totals.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from api.main import app

# Expected counts — overridable via env so CI's fixture (3 stops, 2 communes,
# 4 cells) can pass alongside the production-shaped local dataset.
EXPECTED_STOPS = int(os.getenv("EXPECTED_STOPS", "2793"))
EXPECTED_COMMUNES = int(os.getenv("EXPECTED_COMMUNES", "100"))
EXPECTED_POPULATION_CELLS = int(os.getenv("EXPECTED_POPULATION_CELLS", "2794"))
TEST_STOP_ID = os.getenv("TEST_STOP_ID", "110102007")
TEST_STOP_RESIDENTS_400M = int(os.getenv("TEST_STOP_RESIDENTS_400M", "461"))
TEST_COMMUNE_NAME = os.getenv("TEST_COMMUNE_NAME", "Clervaux")
NOT_A_COMMUNE = os.getenv("NOT_A_COMMUNE", "NotARealCommune")


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


def test_health(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["db"] == "connected"
    assert body["stops"] == EXPECTED_STOPS
    assert body["communes"] == EXPECTED_COMMUNES
    assert body["population_cells"] == EXPECTED_POPULATION_CELLS


def test_ping(client: TestClient) -> None:
    r = client.get("/ping")
    assert r.status_code == 200
    assert r.json() == {"pong": True}


def test_stops(client: TestClient) -> None:
    r = client.get("/stops")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/geo+json")
    assert "max-age=3600" in r.headers["cache-control"]
    fc = r.json()
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == EXPECTED_STOPS
    feat = fc["features"][0]
    assert feat["type"] == "Feature"
    assert feat["geometry"]["type"] == "Point"
    lon, lat = feat["geometry"]["coordinates"]
    assert 5.5 < lon < 6.7, f"longitude {lon} not in LU range"
    assert 49.3 < lat < 50.3, f"latitude {lat} not in LU range"
    assert "stop_id" in feat["properties"]
    assert "stop_name" in feat["properties"]
    assert "vehicle_type" in feat["properties"]
    types = {f["properties"]["vehicle_type"] for f in fc["features"]}
    assert types <= {"bus", "tram", "rail"}, f"unexpected vehicle_types: {types}"


def test_communes(client: TestClient) -> None:
    r = client.get("/communes")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/geo+json")
    assert "max-age=3600" in r.headers["cache-control"]
    fc = r.json()
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == EXPECTED_COMMUNES
    feat = fc["features"][0]
    assert feat["geometry"]["type"] in ("MultiPolygon", "Polygon")
    props = feat["properties"]
    for key in ("commune_id", "name", "canton", "lau2"):
        assert key in props


def test_catchment_ok(client: TestClient) -> None:
    r = client.get(f"/catchment?stop_id={TEST_STOP_ID}&radius=400")
    assert r.status_code == 200
    body = r.json()
    assert body["stop_id"] == TEST_STOP_ID
    assert body["radius_m"] == 400
    assert body["residents"] == TEST_STOP_RESIDENTS_400M
    assert body["cells_intersected"] >= 1
    assert body["catchment_geojson"]["type"] == "Polygon"
    assert body["vehicle_type"] in {"bus", "tram", "rail"}
    parts = (
        body["residents_under15"]
        + body["residents_working_age"]
        + body["residents_over65"]
    )
    assert parts <= body["residents"] + 5  # allow rounding noise


def test_catchment_404_unknown_stop(client: TestClient) -> None:
    r = client.get("/catchment?stop_id=FAKE&radius=400")
    assert r.status_code == 404
    assert "FAKE" in r.json()["detail"]


def test_catchment_422_radius_too_small(client: TestClient) -> None:
    r = client.get(f"/catchment?stop_id={TEST_STOP_ID}&radius=50")
    assert r.status_code == 422


def test_catchment_422_radius_too_large(client: TestClient) -> None:
    r = client.get(f"/catchment?stop_id={TEST_STOP_ID}&radius=99999")
    assert r.status_code == 422


def test_catchment_422_radius_not_int(client: TestClient) -> None:
    r = client.get(f"/catchment?stop_id={TEST_STOP_ID}&radius=abc")
    assert r.status_code == 422


def test_commune_summary_ok(client: TestClient) -> None:
    r = client.get(f"/commune/{TEST_COMMUNE_NAME}/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["commune"] == TEST_COMMUNE_NAME
    assert body["total_stops"] >= 1
    assert body["total_population"] >= 0
    assert body["residents_within_400m"] >= 0
    assert isinstance(body["coverage_pct"], (int, float))
    assert body["stops_geojson"]["type"] == "FeatureCollection"
    assert len(body["stops_geojson"]["features"]) == body["total_stops"]


def test_commune_summary_404(client: TestClient) -> None:
    r = client.get(f"/commune/{NOT_A_COMMUNE}/summary")
    assert r.status_code == 404
