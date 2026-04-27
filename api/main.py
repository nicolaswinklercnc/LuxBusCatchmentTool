import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Luxembourg Bus Catchment API")

PROJECTED_CRS = os.getenv("PROJECTED_CRS", "EPSG:3035")

ALLOWED_ORIGINS = [
    "https://nicolaswinklercnc.github.io",
    "http://localhost:3000",
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "crs": PROJECTED_CRS}


# Fly.io's free tier scale-to-zero is disabled in fly.toml, but as an extra
# safeguard against cold starts an external cron (cron-job.org) pings this
# endpoint every 5 minutes.
@app.get("/ping")
def ping() -> dict:
    return {"pong": True}
