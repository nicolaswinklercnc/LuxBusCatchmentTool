"""Pydantic response models used for OpenAPI docs and runtime validation.

GeoJSON payloads are typed loosely as `dict[str, Any]` since fully modelling
GeoJSON's nested geometry variants in Pydantic is more friction than it's
worth for this project's read-only API.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    db: str
    stops: int | None = None
    population_cells: int | None = None
    communes: int | None = None


class HealthErrorResponse(BaseModel):
    status: str = "error"
    db: str = "error"
    detail: str


class PingResponse(BaseModel):
    pong: bool


class GeoJSONFeatureCollection(BaseModel):
    type: str = Field("FeatureCollection")
    features: list[dict[str, Any]]


class CatchmentResponse(BaseModel):
    stop_id: str
    stop_name: str
    commune: str | None
    vehicle_type: str
    radius_m: int
    residents: int
    residents_under15: int
    residents_working_age: int
    residents_over65: int
    cells_intersected: int
    catchment_geojson: dict[str, Any]


class CommuneSummaryResponse(BaseModel):
    commune: str
    total_stops: int
    total_population: int
    residents_within_400m: int
    coverage_pct: float
    stops_geojson: dict[str, Any]


class ErrorResponse(BaseModel):
    detail: str
