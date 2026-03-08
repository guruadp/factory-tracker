from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import portalocker

from app.config import (
    ACTIVITIES_FILE,
    MASTER_TABLE_FILES,
    OPERATION_LOGS_FILE,
    OPERATORS_FILE,
    STATIONS_FILE,
    TIMEZONE,
    WORK_ORDERS_FILE,
)
from app.models import (
    ActivityRow,
    OperationLogCreate,
    OperatorRow,
    StationRow,
    WorkOrderRow,
)

MASTER_MODELS = {
    "stations": StationRow,
    "activities": ActivityRow,
    "operators": OperatorRow,
    "work_orders": WorkOrderRow,
}

WORK_ORDER_HEADERS = [
    "wo_id",
    "product",
    "planned_qty",
    "status",
    "release_time",
    "due_time",
    "process_stations",
    "created_at",
]

OPERATION_LOG_HEADERS = [
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
    "activity_description",
    "remarks",
    "supervisor_checkin_time",
    "supervisor_checkout_time",
]


class StorageError(ValueError):
    pass


class CSVStorage:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir

    def read_master_table(self, table_name: str) -> list[dict[str, Any]]:
        if table_name not in MASTER_TABLE_FILES:
            raise StorageError(f"Unknown master table: {table_name}")

        rows = self._read_csv_rows(MASTER_TABLE_FILES[table_name])
        model = MASTER_MODELS[table_name]
        return [model.model_validate(row).model_dump() for row in rows]

    def get_stations(self) -> list[dict[str, Any]]:
        rows = self.read_master_table("stations")
        return sorted(rows, key=lambda row: row["station_id"])

    def get_station(self, station_id: str) -> dict[str, Any] | None:
        station_key = self._norm(station_id)
        return next((row for row in self.get_stations() if self._norm(row["station_id"]) == station_key), None)

    def work_order_exists(self, wo_id: str) -> bool:
        wo_key = self._norm(wo_id)
        return any(self._norm(row.get("wo_id", "")) == wo_key for row in self._read_csv_rows(WORK_ORDERS_FILE))

    def get_work_orders(
        self,
        active_only: bool = True,
        q: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.read_master_table("work_orders")

        if active_only:
            rows = [row for row in rows if row["status"] not in {"Completed", "Cancelled"}]

        if status:
            rows = [row for row in rows if row["status"] == status]

        if q:
            q_norm = self._norm(q)
            rows = [
                row
                for row in rows
                if q_norm in self._norm(row["wo_id"]) or q_norm in self._norm(row["product"])
            ]

        def sort_key(row: dict[str, Any]) -> tuple[int, datetime, str]:
            due = row.get("due_time")
            if due is None:
                return (1, datetime.max.replace(tzinfo=TIMEZONE), row["wo_id"])
            return (0, due, row["wo_id"])

        return sorted(rows, key=sort_key)

    def get_work_order(self, wo_id: str) -> dict[str, Any] | None:
        wo_key = self._norm(wo_id)
        for row in self.read_master_table("work_orders"):
            if self._norm(row["wo_id"]) == wo_key:
                return row
        return None

    def append_work_order(self, row_dict: dict[str, Any]) -> dict[str, str]:
        if self.work_order_exists(str(row_dict.get("wo_id", ""))):
            raise StorageError("wo_id already exists")

        row = WorkOrderRow.model_validate(row_dict)
        csv_row = {
            "wo_id": row.wo_id,
            "product": row.product,
            "planned_qty": str(row.planned_qty),
            "status": row.status,
            "release_time": "" if row.release_time is None else row.release_time.astimezone(TIMEZONE).isoformat(),
            "due_time": "" if row.due_time is None else row.due_time.astimezone(TIMEZONE).isoformat(),
            "process_stations": row.process_stations,
            "created_at": row.created_at.astimezone(TIMEZONE).isoformat(),
        }
        self._append_csv_row(WORK_ORDERS_FILE, csv_row, headers=WORK_ORDER_HEADERS)
        return csv_row

    def get_activities_for_station(self, station_id: str) -> list[dict[str, Any]]:
        rows = self.read_master_table("activities")
        station_key = self._norm(station_id)
        filtered = [row for row in rows if self._norm(row["station_id"]) == station_key]
        return sorted(filtered, key=lambda row: row["activity_id"])

    def get_activities_by_station(self, station_id: str) -> list[dict[str, Any]]:
        return self.get_activities_for_station(station_id)

    def get_primary_activity_for_station(self, station_id: str) -> dict[str, Any] | None:
        activities = self.get_activities_for_station(station_id)
        for activity in activities:
            if activity.get("is_primary"):
                return activity
        return None

    def get_operators_for_station(self, station_id: str) -> list[dict[str, Any]]:
        rows = self.read_master_table("operators")
        station_key = self._norm(station_id)
        filtered = [row for row in rows if self._norm(row["station_id"]) == station_key]
        return sorted(filtered, key=lambda row: row["operator_id"])

    def get_operators(self) -> list[dict[str, Any]]:
        rows = self.read_master_table("operators")
        return sorted(rows, key=lambda row: row["operator_id"])

    def get_operators_by_station(self, station_id: str) -> list[dict[str, Any]]:
        return self.get_operators_for_station(station_id)

    def get_logs_for_wo(self, wo_id: str) -> list[dict[str, str]]:
        wo_key = self._norm(wo_id)
        rows = []
        for row in self._read_csv_rows(OPERATION_LOGS_FILE):
            if self._norm(row.get("wo_id", "")) == wo_key:
                rows.append(row)

        def sort_key(row: dict[str, str]) -> tuple[datetime, datetime]:
            start_dt = self._parse_dt(row.get("start_time", ""))
            ts_dt = self._parse_dt(row.get("timestamp_created", ""))
            return (
                start_dt or datetime.max.replace(tzinfo=TIMEZONE),
                ts_dt or datetime.max.replace(tzinfo=TIMEZONE),
            )

        return sorted(rows, key=sort_key)

    def compute_wo_metrics(self, wo: dict[str, Any], logs: list[dict[str, str]]) -> dict[str, Any]:
        total_good = 0
        total_rework = 0
        total_reject = 0
        first_start: datetime | None = None
        last_end: datetime | None = None
        processing_seconds = 0.0
        activity_duration_seconds: dict[str, float] = {}
        reason_breakdown: dict[str, int] = {}

        for row in logs:
            good = self._to_non_negative_int(row.get("qty_good", "0"))
            rework = self._to_non_negative_int(row.get("qty_rework", "0"))
            reject = self._to_non_negative_int(row.get("qty_reject", "0"))
            total_good += good
            total_rework += rework
            total_reject += reject

            start_dt = self._parse_dt(row.get("start_time", ""))
            end_dt = self._parse_dt(row.get("end_time", ""))

            if start_dt is not None:
                if first_start is None or start_dt < first_start:
                    first_start = start_dt

            if end_dt is not None:
                if last_end is None or end_dt > last_end:
                    last_end = end_dt

            if start_dt is not None and end_dt is not None and end_dt >= start_dt:
                seconds = (end_dt - start_dt).total_seconds()
                processing_seconds += seconds
                activity_id = row.get("activity_id", "") or "(unknown)"
                activity_duration_seconds[activity_id] = activity_duration_seconds.get(activity_id, 0.0) + seconds

            if (rework > 0 or reject > 0):
                reason = (row.get("reason_code", "") or "Unspecified").strip() or "Unspecified"
                reason_breakdown[reason] = reason_breakdown.get(reason, 0) + 1

        release_time = wo.get("release_time")
        lead_time_seconds: float | None = None
        if release_time is not None and last_end is not None:
            lead_time_seconds = (last_end - release_time).total_seconds()

        top_bottlenecks = sorted(
            activity_duration_seconds.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:3]

        reason_rows = sorted(reason_breakdown.items(), key=lambda item: item[1], reverse=True)

        return {
            "total_good": total_good,
            "total_rework": total_rework,
            "total_reject": total_reject,
            "first_start": first_start,
            "last_end": last_end,
            "processing_seconds": processing_seconds,
            "lead_time_seconds": lead_time_seconds,
            "top_bottlenecks": top_bottlenecks,
            "reason_breakdown": reason_rows,
        }

    def get_recent_logs(self, limit: int = 10) -> list[dict[str, str]]:
        rows = self._read_csv_rows(OPERATION_LOGS_FILE)
        rows.reverse()
        return rows[:limit]

    def get_recent_logs_for_station(self, station_id: str, limit: int = 10) -> list[dict[str, str]]:
        station_key = self._norm(station_id)
        rows = [
            row for row in self._read_csv_rows(OPERATION_LOGS_FILE) if self._norm(row.get("station_id", "")) == station_key
        ]
        rows.reverse()
        return rows[:limit]

    def get_counts(self) -> dict[str, int]:
        return {
            "stations_count": self._count_rows(STATIONS_FILE),
            "activities_count": self._count_rows(ACTIVITIES_FILE),
            "operators_count": self._count_rows(OPERATORS_FILE),
            "work_orders_count": self._count_rows(WORK_ORDERS_FILE),
            "logs_count": self._count_rows(OPERATION_LOGS_FILE),
        }

    def generate_wo_id(self) -> str:
        existing = {self._norm(row["wo_id"]) for row in self._read_csv_rows(WORK_ORDERS_FILE) if row.get("wo_id")}
        year = datetime.now(TIMEZONE).year
        serial = 1
        while True:
            candidate = f"WO-{year}-{serial:04d}"
            if self._norm(candidate) not in existing:
                return candidate
            serial += 1

    def append_operation_log(self, validated_log_dict: dict[str, Any]) -> dict[str, str]:
        payload = OperationLogCreate.model_validate(validated_log_dict)
        stations = {self._norm(row["station_id"]): row for row in self.get_stations()}
        work_orders = {self._norm(row["wo_id"]): row for row in self.read_master_table("work_orders")}
        activities = {self._norm(row["activity_id"]): row for row in self.read_master_table("activities")}
        operators = {self._norm(row["operator_id"]): row for row in self.read_master_table("operators")}

        station_key = self._norm(payload.station_id)
        wo_key = self._norm(payload.wo_id)
        activity_key = self._norm(payload.activity_id)

        if station_key not in stations:
            raise StorageError(f"station_id '{payload.station_id}' does not exist")

        if wo_key not in work_orders:
            raise StorageError(f"wo_id '{payload.wo_id}' does not exist")

        activity = activities.get(activity_key)
        if activity is None:
            raise StorageError(f"activity_id '{payload.activity_id}' does not exist")
        if self._norm(activity["station_id"]) != station_key:
            raise StorageError("activity_id does not match station_id")

        operator_id = payload.operator_id.strip()
        if operator_id:
            operator = operators.get(self._norm(operator_id))
            if operator is None:
                raise StorageError(f"operator_id '{operator_id}' does not exist")
        else:
            all_operators = self.get_operators()
            if not all_operators:
                raise StorageError("operator_id blank and no operators found")
            operator_id = all_operators[0]["operator_id"]

        row = {
            "log_id": str(uuid4()),
            "timestamp_created": datetime.now(TIMEZONE).isoformat(),
            "supervisor": payload.supervisor,
            "station_id": payload.station_id,
            "wo_id": payload.wo_id,
            "activity_id": payload.activity_id,
            "operator_id": operator_id,
            "start_time": payload.start_time.astimezone(TIMEZONE).isoformat(),
            "end_time": payload.end_time.astimezone(TIMEZONE).isoformat(),
            "qty_good": str(payload.qty_good),
            "qty_rework": str(payload.qty_rework),
            "qty_reject": str(payload.qty_reject),
            "num_operators": str(payload.num_operators),
            "reason_code": payload.reason_code,
            "activity_description": payload.activity_description,
            "remarks": payload.remarks,
            "supervisor_checkin_time": payload.supervisor_checkin_time.astimezone(TIMEZONE).isoformat(),
            "supervisor_checkout_time": datetime.now(TIMEZONE).isoformat(),
        }

        self._append_csv_row(OPERATION_LOGS_FILE, row, headers=OPERATION_LOG_HEADERS)
        return row

    def _append_csv_row(self, path: Path, row: dict[str, str], headers: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with portalocker.Lock(path, mode="a+", newline="", encoding="utf-8", timeout=10) as handle:
            handle.seek(0, 2)
            file_empty = handle.tell() == 0
            writer = csv.DictWriter(handle, fieldnames=headers)
            if file_empty:
                writer.writeheader()
            writer.writerow(row)
            handle.flush()

    @staticmethod
    def _count_rows(path: Path) -> int:
        if not path.exists() or path.stat().st_size == 0:
            return 0
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            return sum(1 for _ in reader)

    @staticmethod
    def _read_csv_rows(path: Path) -> list[dict[str, str]]:
        if not path.exists() or path.stat().st_size == 0:
            return []
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            return [dict(row) for row in reader]

    @staticmethod
    def _norm(value: str) -> str:
        return (value or "").strip().lower()

    @staticmethod
    def _to_non_negative_int(value: str) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 0
        return max(parsed, 0)

    @staticmethod
    def _parse_dt(value: str) -> datetime | None:
        text = (value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
            return parsed.replace(tzinfo=TIMEZONE)
        return parsed.astimezone(TIMEZONE)
