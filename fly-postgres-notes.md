# Fly.io Postgres + PostGIS

The production database is **Fly.io managed Postgres** with the **PostGIS extension** enabled. The API connects via a single `DATABASE_URL` secret.

## Provisioning (one-time)

```bash
# Create the cluster in the same region as the API (cdg = Paris)
flyctl postgres create --name lux-bus-catchment-db --region cdg

# Attach it to the API app — this auto-sets the DATABASE_URL secret
flyctl postgres attach lux-bus-catchment-db --app lux-bus-catchment-api

# Enable PostGIS inside the cluster
flyctl postgres connect -a lux-bus-catchment-db
# then in psql:
#   CREATE EXTENSION IF NOT EXISTS postgis;
#   \q
```

## Setting / rotating secrets

```bash
# Set or override the connection string
flyctl secrets set DATABASE_URL="postgresql://user:pass@host:5432/db" --app lux-bus-catchment-api

# Inspect what's set (values are not printed, only names + digests)
flyctl secrets list --app lux-bus-catchment-api
```

Setting a secret triggers a rolling redeploy of the API.

## Hard rule: no hardcoded credentials

- Never commit DB credentials to git. `.env` is gitignored; `.env.example` carries placeholders only.
- The API reads `DATABASE_URL` from the environment (`os.getenv("DATABASE_URL")`). On Fly that env var is sourced from the secret; locally it comes from `.env` loaded by docker-compose.
- If a credential is ever pushed by accident, rotate it via `flyctl secrets set` **and** rotate the underlying Postgres user inside the cluster.
