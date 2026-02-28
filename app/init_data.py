from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from app.config import DATA_DIR, OPERATION_LOGS_FILE, TIMEZONE


def _iso_now() -> str:
    return datetime.now(TIMEZONE).isoformat()


def _ensure_csv(path: Path, headers: list[str], sample_rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for row in sample_rows:
                writer.writerow(row)


def _migrate_activities_add_is_primary(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        return

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        existing_headers = reader.fieldnames or []
        rows = [dict(row) for row in reader]

    if "is_primary" in existing_headers:
        return

    seen_station: set[str] = set()
    migrated_rows: list[dict[str, str]] = []
    for row in rows:
        station_id = row.get("station_id", "")
        is_primary = "false"
        if station_id not in seen_station:
            is_primary = "true"
            seen_station.add(station_id)

        migrated_rows.append(
            {
                "activity_id": row.get("activity_id", ""),
                "activity_name": row.get("activity_name", ""),
                "station_id": station_id,
                "is_primary": is_primary,
            }
        )

    with path.open("w", newline="", encoding="utf-8") as handle:
        headers = ["activity_id", "activity_name", "station_id", "is_primary"]
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in migrated_rows:
            writer.writerow(row)


def initialize_data_files() -> None:
    now = _iso_now()
    activities_path = DATA_DIR / "activities.csv"

    _ensure_csv(
        DATA_DIR / "stations.csv",
        ["station_id", "station_name", "description"],
        [
            {"station_id": "S01", "station_name": "Assembly", "description": "Primary assembly station"},
            {"station_id": "S02", "station_name": "Inspection", "description": "Quality inspection station"},
            {"station_id": "S03", "station_name": "Packing", "description": "Final packaging"},
        ],
    )

    _ensure_csv(
        activities_path,
        ["activity_id", "activity_name", "station_id", "is_primary"],
        [
            {"activity_id": "A001", "activity_name": "Assemble", "station_id": "S01", "is_primary": "true"},
            {"activity_id": "A002", "activity_name": "Inspect", "station_id": "S02", "is_primary": "true"},
            {"activity_id": "A003", "activity_name": "Pack", "station_id": "S03", "is_primary": "true"},
        ],
    )
    _migrate_activities_add_is_primary(activities_path)

    _ensure_csv(
        DATA_DIR / "operators.csv",
        ["operator_id", "operator_name", "station_id"],
        [
            {"operator_id": "OP01", "operator_name": "Ana Lopez", "station_id": "S01"},
            {"operator_id": "OP02", "operator_name": "Mark Chen", "station_id": "S02"},
            {"operator_id": "OP03", "operator_name": "Sam Patel", "station_id": "S03"},
        ],
    )

    _ensure_csv(
        DATA_DIR / "work_orders.csv",
        ["wo_id", "product", "planned_qty", "status", "release_time", "due_time", "created_at"],
        [
            {
                "wo_id": "WO-2026-0001",
                "product": "Widget-A",
                "planned_qty": "120",
                "status": "In Progress",
                "release_time": now,
                "due_time": now,
                "created_at": now,
            },
            {
                "wo_id": "WO-2026-0002",
                "product": "Widget-B",
                "planned_qty": "80",
                "status": "Not Started",
                "release_time": "",
                "due_time": "",
                "created_at": now,
            },
        ],
    )

    _ensure_csv(
        OPERATION_LOGS_FILE,
        [
            "log_id",
            "timestamp_created",
            "supervisor",
            "station_id",
            "wo_id",
            "activity_id",
            "operator_id",
            "start_time",
            "end_time",
            "qty_good",
            "qty_rework",
            "qty_reject",
            "num_operators",
            "reason_code",
            "remarks",
            "supervisor_checkin_time",
            "supervisor_checkout_time",
        ],
        [
            {
                "log_id": "11111111-1111-4111-8111-111111111111",
                "timestamp_created": now,
                "supervisor": "Supervisor A",
                "station_id": "S01",
                "wo_id": "WO-2026-0001",
                "activity_id": "A001",
                "operator_id": "OP01",
                "start_time": now,
                "end_time": now,
                "qty_good": "10",
                "qty_rework": "0",
                "qty_reject": "0",
                "num_operators": "1",
                "reason_code": "",
                "remarks": "Sample seeded log",
                "supervisor_checkin_time": now,
                "supervisor_checkout_time": now,
            }
        ],
    )


def main() -> None:
    initialize_data_files()
    print(f"CSV initialization complete in {DATA_DIR}")


if __name__ == "__main__":
    main()
