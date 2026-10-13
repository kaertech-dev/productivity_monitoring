# Productivity Monitoring

FastAPI service that reports operator productivity (output qty, target
vs. cycle time, utilization) from the production MySQL databases. One
of the services behind the Manufacturing Dashboard
(`manufacturing_dashboard`), proxied there under `/productivity`.

## Routes

- `GET /` — productivity report page (operator/customer/model grouped,
  with filters by day/week/month/date range and sort)
- `GET /download-csv` — CSV export of the current report
- `GET /api/operator_today` — today's productivity data (JSON)

## Configuration

Database connection is read entirely from the environment — there is no
hardcoded fallback, so the app will fail to start if any of these are
unset:

| Variable | Required | Notes |
|---|---|---|
| `DB_HOST` | yes | MySQL host |
| `DB_USER` | yes | MySQL user |
| `DB_PASSWORD` | yes | MySQL password |

Copy `.env.example` to `.env` and fill in real values before running —
`.env` is git-ignored and read automatically by `docker-compose.yml`.

`hidden_database` (`app/config.py`) excludes databases from per-database
queries/listing (currently `smt`).

## Running

```bash
cp .env.example .env   # fill in DB_HOST / DB_USER / DB_PASSWORD
docker compose up -d --build
```

Serves on port 5001. Requires the external Docker network
`manufacturing_network` to already exist (shared with the other
dashboard services):

```bash
docker network create manufacturing_network
```

## Notes

- Runs as a non-root `appuser` inside the container (set in the
  Dockerfile).
- Requires a `readonly_user`-style MySQL account with `SELECT` access
  across the production databases it reports on.
- See Odoo task 33623 for deployment history/context (migration to
  kts-dashboard CT 124, reading from the kms-web MySQL replica).
