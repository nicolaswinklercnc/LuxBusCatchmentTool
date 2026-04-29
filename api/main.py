"""FastAPI app exposing the Luxembourg bus catchment data.

Endpoints all return GeoJSON or pure JSON; the frontend (MapLibre) consumes
responses without further transformation. Geometry is returned in WGS84,
since MapLibre wants lat/lon — internal queries reproject from EPSG:3035 at
the I/O boundary.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api.db import get_connection, run_geo_query, run_query
from api.models import (
    CatchmentResponse,
    CommuneSummaryResponse,
    ErrorResponse,
    GeoJSONFeatureCollection,
    HealthErrorResponse,
    HealthResponse,
    PingResponse,
)

log = logging.getLogger("luxbus.api")

PROJECTED_CRS = os.getenv("PROJECTED_CRS", "EPSG:3035")

ALLOWED_ORIGINS = [
    "https://nicolaswinklercnc.github.io",
    "http://localhost:3000",
    "http://localhost:8000",
    "http://localhost:5500",
]

CATCHMENT_SQL_FILE = Path(__file__).parent / "sql" / "catchment.sql"
CATCHMENT_SQL = CATCHMENT_SQL_FILE.read_text(encoding="utf-8")
# psycopg2-style %(name)s placeholders → SQLAlchemy :name binds.
CATCHMENT_SQL_SA = (
    CATCHMENT_SQL
    .replace("%(stop_id)s", ":stop_id")
    .replace("%(radius_m)s::integer", ":radius_m_int")
    .replace("%(radius_m)s", ":radius_m")
)

GEOJSON_MEDIA_TYPE = "application/geo+json"
CACHE_1H = "public, max-age=3600"

RADIUS_MIN = 100
RADIUS_MAX = 5000

app = FastAPI(title="Luxembourg Bus Catchment API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.exception_handler(SQLAlchemyError)
async def _db_exception_handler(request: Request, exc: SQLAlchemyError):
    log.exception("database error on %s", request.url.path)
    return JSONResponse(
        status_code=503,
        content={"detail": "database error"},
    )


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        raise exc
    log.exception("unhandled error on %s", request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "internal server error"},
    )


@app.get(
    "/health",
    response_model=HealthResponse,
    responses={503: {"model": HealthErrorResponse}},
)
def health() -> Response:
    try:
        with get_connection() as conn:
            stops = conn.execute(text("SELECT count(*) FROM bus_stops")).scalar_one()
            cells = conn.execute(
                text("SELECT count(*) FROM population_grid")
            ).scalar_one()
            communes = conn.execute(
                text("SELECT count(*) FROM communes")
            ).scalar_one()
    except SQLAlchemyError as exc:
        log.warning("health: db unreachable: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"status": "error", "db": "error", "detail": str(exc)},
        )
    return JSONResponse(
        content={
            "status": "ok",
            "db": "connected",
            "stops": int(stops),
            "population_cells": int(cells),
            "communes": int(communes),
        }
    )


@app.get("/ping", response_model=PingResponse)
def ping() -> dict:
    return {"pong": True}


@app.get(
    "/stops",
    response_model=GeoJSONFeatureCollection,
)
def stops() -> Response:
    fc = run_geo_query(
        """
        SELECT
          ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geom_json,
          stop_id,
          stop_name,
          commune,
          vehicle_type
        FROM bus_stops
        ORDER BY stop_id;
        """
    )
    return Response(
        content=json.dumps(fc),
        media_type=GEOJSON_MEDIA_TYPE,
        headers={"Cache-Control": CACHE_1H},
    )


@app.get(
    "/communes",
    response_model=GeoJSONFeatureCollection,
)
def communes() -> Response:
    fc = run_geo_query(
        """
        SELECT
          ST_AsGeoJSON(ST_Simplify(ST_Transform(geom, 4326), 0.001)) AS geom_json,
          commune_id,
          name,
          canton,
          lau2
        FROM communes
        ORDER BY commune_id;
        """
    )
    return Response(
        content=json.dumps(fc),
        media_type=GEOJSON_MEDIA_TYPE,
        headers={"Cache-Control": CACHE_1H},
    )


@app.get(
    "/catchment",
    response_model=CatchmentResponse,
    responses={
        404: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
    },
)
def catchment(
    stop_id: str = Query(..., min_length=1),
    radius: int = Query(..., ge=RADIUS_MIN, le=RADIUS_MAX),
) -> CatchmentResponse:
    rows = run_query(
        CATCHMENT_SQL_SA,
        {"stop_id": stop_id, "radius_m": radius, "radius_m_int": radius},
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"stop_id '{stop_id}' not found",
        )
    row = rows[0]
    return CatchmentResponse(
        stop_id=row["stop_id"],
        stop_name=row["stop_name"],
        commune=row["commune"],
        vehicle_type=row["vehicle_type"],
        radius_m=int(row["radius_m"]),
        residents=int(row["residents"]),
        residents_under15=int(row["residents_under15"]),
        residents_working_age=int(row["residents_working_age"]),
        residents_over65=int(row["residents_over65"]),
        cells_intersected=int(row["cells_intersected"]),
        catchment_geojson=json.loads(row["catchment_geojson"]),
    )


@app.get(
    "/commune/{name}/summary",
    response_model=CommuneSummaryResponse,
    responses={404: {"model": ErrorResponse}},
)
def commune_summary(name: str) -> CommuneSummaryResponse:
    with get_connection() as conn:
        commune_row = conn.execute(
            text(
                """
                SELECT name, geom
                FROM communes
                WHERE name = :name
                LIMIT 1;
                """
            ),
            {"name": name},
        ).first()
        if commune_row is None:
            raise HTTPException(
                status_code=404, detail=f"commune '{name}' not found"
            )

        total_stops = conn.execute(
            text("SELECT count(*) FROM bus_stops WHERE commune = :name"),
            {"name": name},
        ).scalar_one()

        total_population = conn.execute(
            text(
                """
                SELECT COALESCE(SUM(pg.pop_count), 0)
                FROM population_grid pg, communes c
                WHERE c.name = :name AND ST_Intersects(pg.geom, c.geom);
                """
            ),
            {"name": name},
        ).scalar_one()

        residents_within = conn.execute(
            text(
                """
                SELECT COALESCE(SUM(pg.pop_count), 0)
                FROM population_grid pg
                WHERE ST_Intersects(
                  pg.geom,
                  (SELECT ST_Union(ST_Buffer(geom, 400))
                     FROM bus_stops WHERE commune = :name)
                );
                """
            ),
            {"name": name},
        ).scalar_one()

    fc = run_geo_query(
        """
        SELECT
          ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geom_json,
          stop_id,
          stop_name,
          commune,
          vehicle_type
        FROM bus_stops
        WHERE commune = :name
        ORDER BY stop_id;
        """,
        {"name": name},
    )

    pop = int(total_population)
    within = int(residents_within)
    pct = round((within / pop * 100), 1) if pop else 0.0
    return CommuneSummaryResponse(
        commune=name,
        total_stops=int(total_stops),
        total_population=pop,
        residents_within_400m=within,
        coverage_pct=pct,
        stops_geojson=fc,
    )


@app.get(
    "/cycling",
    response_model=GeoJSONFeatureCollection,
)
def cycling() -> Response:
    fc = run_geo_query(
        """
        SELECT
          ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geom_json,
          osm_id,
          category,
          source,
          name,
          surface
        FROM cycling_infrastructure
        ORDER BY feature_id;
        """
    )
    return Response(
        content=json.dumps(fc),
        media_type=GEOJSON_MEDIA_TYPE,
        headers={"Cache-Control": CACHE_1H},
    )


@app.get("/cycling/summary")
def cycling_summary() -> dict:
    """Per-category feature count + km, plus total km. Used by the frontend
    filter UI to show '(NN km)' suffixes."""
    rows = run_query(
        """
        SELECT
          category,
          COUNT(*) AS count,
          ROUND((COALESCE(SUM(ST_Length(geom)), 0) / 1000.0)::numeric, 1) AS km
        FROM cycling_infrastructure
        GROUP BY category;
        """
    )
    out: dict = {
        c: {"count": 0, "km": 0.0} for c in ("segregated", "shared", "planned")
    }
    for r in rows:
        cat = r["category"]
        if cat in out:
            out[cat] = {"count": int(r["count"]), "km": float(r["km"])}
    out["total_km"] = round(
        sum(v["km"] for v in out.values() if isinstance(v, dict)), 1
    )
    return out
