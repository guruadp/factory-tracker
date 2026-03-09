from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import func, select

from app.config import TIMEZONE
from app.db import session_scope
from app.db_models import Activity, OperationLog, Operator, Station, WorkOrder
from app.models import OperationLogCreate, WorkOrderRow
from app.storage import StorageError


class DBStorage:
    def read_master_table(self, table_name: str) -> list[dict[str, Any]]:
        with session_scope() as session:
            if table_name == "stations":
                rows = session.execute(select(Station)).scalars().all()
                return [
                    {
                        "station_id": row.station_id,
                        "station_name": row.station_name,
                        "description": row.description or "",
                    }
                    for row in rows
                ]
            if table_name == "activities":
                rows = session.execute(select(Activity)).scalars().all()
                return [
                    {
                        "activity_id": row.activity_id,
                        "activity_name": row.activity_name,
                        "station_id": row.station_id,
                        "is_primary": bool(row.is_primary),
                    }
                    for row in rows
                ]
            if table_name == "operators":
                rows = session.execute(select(Operator)).scalars().all()
                return [
                    {
                        "operator_id": row.operator_id,
                        "operator_name": row.operator_name,
                        "station_id": row.station_id,
                    }
                    for row in rows
                ]
            if table_name == "work_orders":
                rows = session.execute(select(WorkOrder)).scalars().all()
                return [self._work_order_to_dict(row) for row in rows]
        raise StorageError(f"Unknown master table: {table_name}")

    def get_stations(self) -> list[dict[str, Any]]:
        rows = self.read_master_table("stations")
        return sorted(rows, key=lambda row: row["station_id"])

    def get_station(self, station_id: str) -> dict[str, Any] | None:
        key = self._norm(station_id)
        return next((row for row in self.get_stations() if self._norm(row["station_id"]) == key), None)

    def work_order_exists(self, wo_id: str) -> bool:
        wo_key = self._norm(wo_id)
        with session_scope() as session:
            row = session.execute(select(WorkOrder.wo_id)).scalars().all()
        return any(self._norm(item) == wo_key for item in row)

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
        key = self._norm(wo_id)
        for row in self.get_work_orders(active_only=False):
            if self._norm(row["wo_id"]) == key:
                return row
        return None

    def append_work_order(self, row_dict: dict[str, Any]) -> dict[str, str]:
        if self.work_order_exists(str(row_dict.get("wo_id", ""))):
            raise StorageError("wo_id already exists")

        row = WorkOrderRow.model_validate(row_dict)
        with session_scope() as session:
            db_row = WorkOrder(
                wo_id=row.wo_id,
                product=row.product,
                planned_qty=row.planned_qty,
                status=row.status,
                release_time=row.release_time,
                due_time=row.due_time,
                process_stations=row.process_stations,
                created_at=row.created_at,
            )
            session.add(db_row)

        return {
            "wo_id": row.wo_id,
            "product": row.product,
            "planned_qty": str(row.planned_qty),
            "status": row.status,
            "release_time": "" if row.release_time is None else row.release_time.astimezone(TIMEZONE).isoformat(),
            "due_time": "" if row.due_time is None else row.due_time.astimezone(TIMEZONE).isoformat(),
            "process_stations": row.process_stations,
            "created_at": row.created_at.astimezone(TIMEZONE).isoformat(),
        }

    def get_activities_for_station(self, station_id: str) -> list[dict[str, Any]]:
        key = self._norm(station_id)
        rows = self.read_master_table("activities")
        filtered = [row for row in rows if self._norm(row["station_id"]) == key]
        return sorted(filtered, key=lambda row: row["activity_id"])

    def get_activities_by_station(self, station_id: str) -> list[dict[str, Any]]:
        return self.get_activities_for_station(station_id)

    def get_primary_activity_for_station(self, station_id: str) -> dict[str, Any] | None:
        for item in self.get_activities_for_station(station_id):
            if item.get("is_primary"):
                return item
        return None

    def get_operators_for_station(self, station_id: str) -> list[dict[str, Any]]:
        key = self._norm(station_id)
        rows = self.read_master_table("operators")
        filtered = [row for row in rows if self._norm(row["station_id"]) == key]
        return sorted(filtered, key=lambda row: row["operator_id"])

    def get_operators(self) -> list[dict[str, Any]]:
        rows = self.read_master_table("operators")
        return sorted(rows, key=lambda row: row["operator_id"])

    def get_operators_by_station(self, station_id: str) -> list[dict[str, Any]]:
        return self.get_operators_for_station(station_id)

    def get_logs_for_wo(self, wo_id: str) -> list[dict[str, str]]:
        key = self._norm(wo_id)
        with session_scope() as session:
            rows = session.execute(select(OperationLog)).scalars().all()
        filtered = [self._log_to_dict(row) for row in rows if self._norm(row.wo_id) == key]
        return sorted(
            filtered,
            key=lambda row: (
                self._parse_dt(row.get("start_time", "")) or datetime.max.replace(tzinfo=TIMEZONE),
                self._parse_dt(row.get("timestamp_created", "")) or datetime.max.replace(tzinfo=TIMEZONE),
            ),
        )

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
            if start_dt is not None and (first_start is None or start_dt < first_start):
                first_start = start_dt
            if end_dt is not None and (last_end is None or end_dt > last_end):
                last_end = end_dt
            if start_dt is not None and end_dt is not None and end_dt >= start_dt:
                seconds = (end_dt - start_dt).total_seconds()
                processing_seconds += seconds
                activity_id = row.get("activity_id", "") or "(unknown)"
                activity_duration_seconds[activity_id] = activity_duration_seconds.get(activity_id, 0.0) + seconds

            if rework > 0 or reject > 0:
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
        with session_scope() as session:
            rows = session.execute(select(OperationLog).order_by(OperationLog.timestamp_created.desc()).limit(limit)).scalars().all()
        return [self._log_to_dict(row) for row in rows]

    def get_recent_logs_for_station(self, station_id: str, limit: int = 10) -> list[dict[str, str]]:
        key = self._norm(station_id)
        with session_scope() as session:
            rows = session.execute(
                select(OperationLog).where(func.lower(OperationLog.station_id) == key).order_by(OperationLog.timestamp_created.desc()).limit(limit)
            ).scalars().all()
        return [self._log_to_dict(row) for row in rows]

    def get_counts(self) -> dict[str, int]:
        with session_scope() as session:
            return {
                "stations_count": session.scalar(select(func.count()).select_from(Station)) or 0,
                "activities_count": session.scalar(select(func.count()).select_from(Activity)) or 0,
                "operators_count": session.scalar(select(func.count()).select_from(Operator)) or 0,
                "work_orders_count": session.scalar(select(func.count()).select_from(WorkOrder)) or 0,
                "logs_count": session.scalar(select(func.count()).select_from(OperationLog)) or 0,
            }

    def append_operation_log(self, validated_log_dict: dict[str, Any]) -> dict[str, str]:
        payload = OperationLogCreate.model_validate(validated_log_dict)
        with session_scope() as session:
            station = session.get(Station, payload.station_id)
            if station is None:
                raise StorageError(f"station_id '{payload.station_id}' does not exist")

            wo = session.get(WorkOrder, payload.wo_id)
            if wo is None:
                raise StorageError(f"wo_id '{payload.wo_id}' does not exist")

            activity = session.get(Activity, payload.activity_id)
            if activity is None:
                raise StorageError(f"activity_id '{payload.activity_id}' does not exist")
            if self._norm(activity.station_id) != self._norm(payload.station_id):
                raise StorageError("activity_id does not match station_id")

            operator_id = payload.operator_id.strip()
            if operator_id:
                operator = session.get(Operator, operator_id)
                if operator is None:
                    raise StorageError(f"operator_id '{operator_id}' does not exist")
            else:
                operator = session.execute(select(Operator).order_by(Operator.operator_id)).scalars().first()
                if operator is None:
                    raise StorageError("operator_id blank and no operators found")
                operator_id = operator.operator_id

            row = OperationLog(
                log_id=str(uuid4()),
                timestamp_created=datetime.now(TIMEZONE),
                supervisor=payload.supervisor,
                station_id=payload.station_id,
                wo_id=payload.wo_id,
                activity_id=payload.activity_id,
                operator_id=operator_id,
                start_time=payload.start_time.astimezone(TIMEZONE),
                end_time=payload.end_time.astimezone(TIMEZONE),
                qty_good=payload.qty_good,
                qty_rework=payload.qty_rework,
                qty_reject=payload.qty_reject,
                num_operators=payload.num_operators,
                reason_code=payload.reason_code,
                activity_description=payload.activity_description,
                remarks=payload.remarks,
                supervisor_checkin_time=payload.supervisor_checkin_time.astimezone(TIMEZONE),
                supervisor_checkout_time=datetime.now(TIMEZONE),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self._log_to_dict(row)

    @staticmethod
    def _work_order_to_dict(row: WorkOrder) -> dict[str, Any]:
        return {
            "wo_id": row.wo_id,
            "product": row.product,
            "planned_qty": row.planned_qty,
            "status": row.status,
            "release_time": row.release_time,
            "due_time": row.due_time,
            "process_stations": row.process_stations or "",
            "created_at": row.created_at,
        }

    @staticmethod
    def _log_to_dict(row: OperationLog) -> dict[str, str]:
        return {
            "log_id": row.log_id,
            "timestamp_created": row.timestamp_created.astimezone(TIMEZONE).isoformat(),
            "supervisor": row.supervisor,
            "station_id": row.station_id,
            "wo_id": row.wo_id,
            "activity_id": row.activity_id,
            "operator_id": row.operator_id,
            "start_time": row.start_time.astimezone(TIMEZONE).isoformat(),
            "end_time": row.end_time.astimezone(TIMEZONE).isoformat(),
            "qty_good": str(row.qty_good),
            "qty_rework": str(row.qty_rework),
            "qty_reject": str(row.qty_reject),
            "num_operators": str(row.num_operators),
            "reason_code": row.reason_code or "",
            "activity_description": row.activity_description or "",
            "remarks": row.remarks or "",
            "supervisor_checkin_time": row.supervisor_checkin_time.astimezone(TIMEZONE).isoformat(),
            "supervisor_checkout_time": row.supervisor_checkout_time.astimezone(TIMEZONE).isoformat(),
        }

    @staticmethod
    def _norm(value: str) -> str:
        return (value or "").strip().lower()

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

    @staticmethod
    def _to_non_negative_int(value: str) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 0
        return max(parsed, 0)
