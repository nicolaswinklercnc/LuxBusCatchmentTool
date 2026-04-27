import os

from fastapi import FastAPI

app = FastAPI(title="Luxembourg Bus Catchment API")

PROJECTED_CRS = os.getenv("PROJECTED_CRS", "EPSG:3035")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "crs": PROJECTED_CRS}
