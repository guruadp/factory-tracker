from __future__ import annotations

import csv
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Generator, Iterable

from app.config import OPERATION_LOGS_FILE, TIMEZONE


def parse_dt(value: str | None) -> datetime | None:
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


def to_int(value: str | None) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def iter_operation_logs(path: Path = OPERATION_LOGS_FILE) -> Generator[dict[str, str], None, None]:
    if not path.exists() or path.stat().st_size == 0:
        return
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield dict(row)


def filter_logs_by_date(logs_iter: Iterable[dict[str, str]], target_date: date) -> Generator[dict[str, str], None, None]:
    for row in logs_iter:
        ts = parse_dt(row.get("timestamp_created", ""))
        if ts is not None and ts.date() == target_date:
            yield row


def get_logs_for_station(station_id: str, limit: int | None = None) -> list[dict[str, str]]:
    key = _norm(station_id)
    rows: list[dict[str, str]] = []
    for row in iter_operation_logs():
        if _norm(row.get("station_id", "")) == key:
            rows.append(row)

    rows.sort(
        key=lambda row: parse_dt(row.get("timestamp_created", "")) or datetime.min.replace(tzinfo=TIMEZONE),
        reverse=True,
    )
    if limit is not None:
        return rows[:limit]
    return rows


def compute_station_metrics(
    station_id: str,
    target_date: date,
    window_hours: int,
    stale_hours: int,
    stations: list[dict[str, Any]],
    work_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    all_metrics = compute_all_stations_metrics(
        target_date=target_date,
        window_hours=window_hours,
        stale_hours=stale_hours,
        stations=stations,
        work_orders=work_orders,
    )
    key = _norm(station_id)
    for metric in all_metrics:
        if _norm(metric["station_id"]) == key:
            return metric
    return _empty_station_metrics(station_id=station_id, station_name=station_id)


def compute_all_stations_metrics(
    target_date: date,
    window_hours: int,
    stale_hours: int,
    stations: list[dict[str, Any]],
    work_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    now = datetime.now(TIMEZONE)
    day_start = datetime.combine(target_date, time.min, tzinfo=TIMEZONE)
    day_end = datetime.combine(target_date, time.max, tzinfo=TIMEZONE)
    is_today = target_date == now.date()

    if is_today:
        wip_start = now - timedelta(hours=window_hours)
        wip_end = now
        stale_reference = now
    else:
        wip_start = day_start
        wip_end = day_end
        stale_reference = day_end

    stale_cutoff = stale_reference - timedelta(hours=stale_hours)

    wo_status_map = {_norm(row["wo_id"]): row.get("status", "") for row in work_orders}

    per_station: dict[str, dict[str, Any]] = {
        _norm(st["station_id"]): _empty_station_metrics(st["station_id"], st.get("station_name", st["station_id"]))
        for st in stations
    }
    latest_station_wo: dict[tuple[str, str], tuple[datetime, dict[str, str]]] = {}

    for row in iter_operation_logs():
        station_id = row.get("station_id", "")
        station_key = _norm(station_id)
        if station_key not in per_station:
            per_station[station_key] = _empty_station_metrics(station_id=station_id, station_name=station_id)

        ts = parse_dt(row.get("timestamp_created", ""))
        if ts is None:
            continue

        metric = per_station[station_key]

        if ts.date() == target_date:
            qty_good = max(0, to_int(row.get("qty_good", "0")))
            qty_rework = max(0, to_int(row.get("qty_rework", "0")))
            qty_reject = max(0, to_int(row.get("qty_reject", "0")))

            metric["logs_count_today"] += 1
            metric["qty_good_today"] += qty_good
            metric["qty_rework_today"] += qty_rework
            metric["qty_reject_today"] += qty_reject

            start_dt = parse_dt(row.get("start_time", ""))
            end_dt = parse_dt(row.get("end_time", ""))
            if start_dt is not None and end_dt is not None and end_dt >= start_dt:
                minutes = (end_dt - start_dt).total_seconds() / 60.0
                metric["total_reported_processing_minutes_today"] += minutes
                metric["_duration_count"] += 1

        wo_key = _norm(row.get("wo_id", ""))
        if wo_key and wip_start <= ts <= wip_end:
            current = latest_station_wo.get((station_key, wo_key))
            if current is None or ts > current[0]:
                latest_station_wo[(station_key, wo_key)] = (ts, row)

    for (station_key, wo_key), (last_ts, _) in latest_station_wo.items():
        status = wo_status_map.get(wo_key, "")
        if status in {"Completed", "Cancelled"}:
            continue
        metric = per_station[station_key]
        metric["wip_count"] += 1
        if last_ts < stale_cutoff:
            metric["stale_wip_count"] += 1

    metrics = []
    for metric in per_station.values():
        count = metric.pop("_duration_count")
        if count > 0:
            metric["avg_cycle_minutes_today"] = metric["total_reported_processing_minutes_today"] / count
        else:
            metric["avg_cycle_minutes_today"] = 0.0
        metrics.append(metric)

    metrics.sort(key=lambda item: item["station_id"])
    return metrics


def compute_station_detail_tables(
    station_id: str,
    target_date: date,
    window_hours: int,
    stale_hours: int,
    work_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    now = datetime.now(TIMEZONE)
    day_start = datetime.combine(target_date, time.min, tzinfo=TIMEZONE)
    day_end = datetime.combine(target_date, time.max, tzinfo=TIMEZONE)
    is_today = target_date == now.date()

    if is_today:
        wip_start = now - timedelta(hours=window_hours)
        wip_end = now
        stale_reference = now
    else:
        wip_start = day_start
        wip_end = day_end
        stale_reference = day_end

    stale_cutoff = stale_reference - timedelta(hours=stale_hours)
    station_key = _norm(station_id)

    wo_map = {_norm(row["wo_id"]): row for row in work_orders}
    recent_logs: list[dict[str, str]] = []
    latest_by_wo: dict[str, tuple[datetime, dict[str, str]]] = {}
    good_by_wo_today: dict[str, int] = {}
    reason_rollup: dict[str, dict[str, int]] = {}

    for row in iter_operation_logs():
        if _norm(row.get("station_id", "")) != station_key:
            continue

        ts = parse_dt(row.get("timestamp_created", ""))
        if ts is None:
            continue

        recent_logs.append(row)

        wo_key = _norm(row.get("wo_id", ""))
        if wo_key and wip_start <= ts <= wip_end:
            current = latest_by_wo.get(wo_key)
            if current is None or ts > current[0]:
                latest_by_wo[wo_key] = (ts, row)

        if ts.date() == target_date:
            qty_good = max(0, to_int(row.get("qty_good", "0")))
            good_by_wo_today[wo_key] = good_by_wo_today.get(wo_key, 0) + qty_good

            qty_rework = max(0, to_int(row.get("qty_rework", "0")))
            qty_reject = max(0, to_int(row.get("qty_reject", "0")))
            if qty_rework > 0 or qty_reject > 0:
                reason = (row.get("reason_code", "") or "Unspecified").strip() or "Unspecified"
                bucket = reason_rollup.setdefault(reason, {"count": 0, "qty_rework": 0, "qty_reject": 0})
                bucket["count"] += 1
                bucket["qty_rework"] += qty_rework
                bucket["qty_reject"] += qty_reject

    recent_logs.sort(
        key=lambda row: parse_dt(row.get("timestamp_created", "")) or datetime.min.replace(tzinfo=TIMEZONE),
        reverse=True,
    )

    active_rows: list[dict[str, Any]] = []
    for wo_key, (last_ts, row) in latest_by_wo.items():
        wo = wo_map.get(wo_key)
        status = wo.get("status", "") if wo else ""
        if status in {"Completed", "Cancelled"}:
            continue
        active_rows.append(
            {
                "wo_id": row.get("wo_id", ""),
                "product": wo.get("product", "") if wo else "",
                "last_activity": row.get("activity_id", ""),
                "last_update_time": last_ts,
                "last_supervisor": row.get("supervisor", ""),
                "qty_good_total_at_station_today": good_by_wo_today.get(wo_key, 0),
                "stale_flag": last_ts < stale_cutoff,
            }
        )

    active_rows.sort(key=lambda item: item["last_update_time"], reverse=True)

    reason_rows = [
        {
            "reason_code": reason,
            "count": values["count"],
            "qty_rework": values["qty_rework"],
            "qty_reject": values["qty_reject"],
        }
        for reason, values in sorted(reason_rollup.items(), key=lambda item: item[1]["count"], reverse=True)
    ]

    return {
        "active_wos": active_rows,
        "recent_logs": recent_logs[:50],
        "reasons": reason_rows,
    }


def compute_bottlenecks(metrics_list: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
    mapping = {
        "processing": "total_reported_processing_minutes_today",
        "wip": "wip_count",
        "stale": "stale_wip_count",
        "rejects": "qty_reject_today",
    }
    key = mapping.get(metric, "total_reported_processing_minutes_today")
    ranked = sorted(metrics_list, key=lambda item: item.get(key, 0), reverse=True)
    return ranked[:10]


def count_work_orders_by_status(work_orders: list[dict[str, Any]]) -> dict[str, int]:
    statuses = ["Not Started", "In Progress", "On Hold", "Completed", "Cancelled"]
    counts = {status: 0 for status in statuses}
    for row in work_orders:
        status = row.get("status", "")
        if status in counts:
            counts[status] += 1
    return counts


def aggregate_rejects_rework_by_station(
    target_date: date,
    stations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    station_name_map = { _norm(st["station_id"]): st.get("station_name", st["station_id"]) for st in stations }
    rollup: dict[str, dict[str, Any]] = {}

    for row in iter_operation_logs():
        ts = parse_dt(row.get("timestamp_created", ""))
        if ts is None or ts.date() != target_date:
            continue

        station_id = row.get("station_id", "")
        station_key = _norm(station_id)
        bucket = rollup.setdefault(
            station_key,
            {
                "station_id": station_id,
                "station_name": station_name_map.get(station_key, station_id),
                "rejects": 0,
                "rework": 0,
            },
        )
        bucket["rejects"] += max(0, to_int(row.get("qty_reject", "0")))
        bucket["rework"] += max(0, to_int(row.get("qty_rework", "0")))

    rows = list(rollup.values())
    rows.sort(key=lambda item: item["station_id"])
    return rows


def aggregate_processing_minutes_by_station(
    target_date: date,
    stations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    station_name_map = { _norm(st["station_id"]): st.get("station_name", st["station_id"]) for st in stations }
    rollup: dict[str, dict[str, Any]] = {}

    for row in iter_operation_logs():
        ts = parse_dt(row.get("timestamp_created", ""))
        if ts is None or ts.date() != target_date:
            continue

        start_dt = parse_dt(row.get("start_time", ""))
        end_dt = parse_dt(row.get("end_time", ""))
        if start_dt is None or end_dt is None or end_dt < start_dt:
            continue

        station_id = row.get("station_id", "")
        station_key = _norm(station_id)
        bucket = rollup.setdefault(
            station_key,
            {
                "station_id": station_id,
                "station_name": station_name_map.get(station_key, station_id),
                "processing_minutes": 0.0,
            },
        )
        bucket["processing_minutes"] += (end_dt - start_dt).total_seconds() / 60.0

    rows = list(rollup.values())
    rows.sort(key=lambda item: item["processing_minutes"], reverse=True)
    return rows


def _empty_station_metrics(station_id: str, station_name: str) -> dict[str, Any]:
    return {
        "station_id": station_id,
        "station_name": station_name,
        "logs_count_today": 0,
        "qty_good_today": 0,
        "qty_rework_today": 0,
        "qty_reject_today": 0,
        "total_reported_processing_minutes_today": 0.0,
        "avg_cycle_minutes_today": 0.0,
        "wip_count": 0,
        "stale_wip_count": 0,
        "_duration_count": 0,
    }


def _norm(value: str) -> str:
    return (value or "").strip().lower()
