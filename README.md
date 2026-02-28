# Planning Dashboard (Phase-0)

Lightweight FastAPI app for manufacturing operation logs with CSV storage.

## Stack

- FastAPI + Uvicorn
- Jinja2 templates + Tailwind CDN
- CSV-backed storage (`pandas` for reads, `csv` + `portalocker` for append writes)
- Pydantic validation

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open: http://127.0.0.1:8000/logs/new

## Data Files

- `data/stations.csv`
- `data/activities.csv`
- `data/operators.csv`
- `data/work_orders.csv`
- `data/operation_logs.csv` (created on first write)

## Current Views

- Quick New Log: `/logs/new`
- Health: `/health`

## Notes

- `operation_logs.csv` is append-only.
- Writes use file locking (`portalocker`) for basic concurrency safety.
- Data model is intentionally relational-like to simplify migration to PostgreSQL later.
