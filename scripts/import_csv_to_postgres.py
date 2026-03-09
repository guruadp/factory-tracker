from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from app.config import BASE_DIR, DATABASE_URL, TIMEZONE
from app.db import ensure_database_schema, is_database_enabled, session_scope
from app.db_models import Activity, OperationLog, Operator, Station, WorkOrder


def _parse_dt(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return parsed.replace(tzinfo=TIMEZONE)
    return parsed.astimezone(TIMEZONE)


def _read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def main() -> None:
    if not is_database_enabled():
        raise RuntimeError("Set DATABASE_URL before running import")

    data_dir = BASE_DIR / "data"
    ensure_database_schema()

    stations = _read_rows(data_dir / "stations.csv")
    activities = _read_rows(data_dir / "activities.csv")
    operators = _read_rows(data_dir / "operators.csv")
    work_orders = _read_rows(data_dir / "work_orders.csv")
    operation_logs = _read_rows(data_dir / "operation_logs.csv")

    with session_scope() as session:
        session.query(OperationLog).delete()
        session.query(WorkOrder).delete()
        session.query(Operator).delete()
        session.query(Activity).delete()
        session.query(Station).delete()

        for row in stations:
            session.add(
                Station(
                    station_id=row.get("station_id", ""),
                    station_name=row.get("station_name", ""),
                    description=row.get("description", "") or "",
                )
            )

        for row in activities:
            session.add(
                Activity(
                    activity_id=row.get("activity_id", ""),
                    activity_name=row.get("activity_name", ""),
                    station_id=row.get("station_id", ""),
                    is_primary=(row.get("is_primary", "").strip().lower() == "true"),
                )
            )

        for row in operators:
            session.add(
                Operator(
                    operator_id=row.get("operator_id", ""),
                    operator_name=row.get("operator_name", ""),
                    station_id=row.get("station_id", ""),
                )
            )

        for row in work_orders:
            session.add(
                WorkOrder(
                    wo_id=row.get("wo_id", ""),
                    product=row.get("product", ""),
                    planned_qty=max(1, int(row.get("planned_qty", "1") or 1)),
                    status=row.get("status", "Not Started") or "Not Started",
                    release_time=_parse_dt(row.get("release_time", "")),
                    due_time=_parse_dt(row.get("due_time", "")),
                    process_stations=row.get("process_stations", "") or "",
                    created_at=_parse_dt(row.get("created_at", "")) or datetime.now(TIMEZONE),
                )
            )

        for row in operation_logs:
            session.add(
                OperationLog(
                    log_id=row.get("log_id", ""),
                    timestamp_created=_parse_dt(row.get("timestamp_created", "")) or datetime.now(TIMEZONE),
                    supervisor=row.get("supervisor", ""),
                    station_id=row.get("station_id", ""),
                    wo_id=row.get("wo_id", ""),
                    activity_id=row.get("activity_id", ""),
                    operator_id=row.get("operator_id", ""),
                    start_time=_parse_dt(row.get("start_time", "")) or datetime.now(TIMEZONE),
                    end_time=_parse_dt(row.get("end_time", "")) or datetime.now(TIMEZONE),
                    qty_good=max(0, int(row.get("qty_good", "0") or 0)),
                    qty_rework=max(0, int(row.get("qty_rework", "0") or 0)),
                    qty_reject=max(0, int(row.get("qty_reject", "0") or 0)),
                    num_operators=max(1, int(row.get("num_operators", "1") or 1)),
                    reason_code=row.get("reason_code", "") or "",
                    activity_description=row.get("activity_description", "") or "",
                    remarks=row.get("remarks", "") or "",
                    supervisor_checkin_time=_parse_dt(row.get("supervisor_checkin_time", "")) or datetime.now(TIMEZONE),
                    supervisor_checkout_time=_parse_dt(row.get("supervisor_checkout_time", "")) or datetime.now(TIMEZONE),
                )
            )

    print(f"Imported CSV data into Postgres using DATABASE_URL='{DATABASE_URL[:24]}...'")


if __name__ == "__main__":
    main()
