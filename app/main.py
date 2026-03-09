from __future__ import annotations

import csv
import io
from datetime import date, datetime, timedelta
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError

from app.analytics import (
    aggregate_processing_minutes_by_station,
    aggregate_rejects_rework_by_station,
    count_work_orders_by_status,
    compute_all_stations_metrics,
    compute_bottlenecks,
    iter_operation_logs,
    parse_dt,
    compute_station_detail_tables,
    compute_station_metrics,
    to_int,
)
from app.config import (
    BASE_DIR,
    DEFAULT_STALE_HOURS,
    DEFAULT_WINDOW_HOURS,
    REASON_CODES,
    START_TIME_OFFSET_MINUTES,
    TIMEZONE,
)
from app.init_data import initialize_data_files
from app.models import OperationLogCreate, WorkOrderCreate
from app.storage import CSVStorage, StorageError

app = FastAPI(title="Manufacturing WO Tracker", version="0.2.0")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))
storage = CSVStorage(data_dir=BASE_DIR / "data")
app.mount("/assets", StaticFiles(directory=str(BASE_DIR / "data")), name="assets")


@app.on_event("startup")
def startup_event() -> None:
    initialize_data_files()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _format_dt_local(value: datetime) -> str:
    return value.astimezone(TIMEZONE).strftime("%Y-%m-%dT%H:%M")


def _now_dubai() -> datetime:
    return datetime.now(TIMEZONE)


def _parse_datetime_input(value: str) -> datetime:
    text = (value or "").strip()
    if not text:
        raise ValueError("Datetime value is required")

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("Invalid datetime format") from exc

    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        parsed = parsed.replace(tzinfo=TIMEZONE)
    else:
        parsed = parsed.astimezone(TIMEZONE)
    return parsed


def _parse_optional_datetime_input(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    return _parse_datetime_input(text)


def _format_dt_display(value: datetime | None) -> str:
    if value is None:
        return "N/A"
    return value.astimezone(TIMEZONE).strftime("%Y-%m-%d %H:%M")


def _format_seconds_human(seconds: float | None) -> str:
    if seconds is None:
        return "N/A"
    total = int(max(seconds, 0))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}h {minutes}m {secs}s"


def _safe_parse_datetime(value: str) -> datetime | None:
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


def _errors_by_field(exc: ValidationError) -> dict[str, list[str]]:
    errors: dict[str, list[str]] = {}
    for err in exc.errors():
        loc = err.get("loc", [])
        field = str(loc[-1]) if loc else "__all__"
        errors.setdefault(field, []).append(err.get("msg", "Invalid value"))
    return errors


def _precedence_activities() -> list[str]:
    precedence_file = BASE_DIR / "data" / "Precedence and Suceedance.csv"
    if not precedence_file.exists() or precedence_file.stat().st_size == 0:
        return []

    activities: list[str] = []
    seen: set[str] = set()
    with precedence_file.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            name = (row[0] or "").strip()
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            activities.append(name)
    return activities


def _build_form_context(
    request: Request,
    station_id: str,
    values: dict[str, Any],
    errors: dict[str, list[str]] | None = None,
    form_error: str | None = None,
) -> dict[str, Any]:
    stations = storage.get_stations()
    station = next((item for item in stations if item["station_id"] == station_id), None)

    if station is None:
        return {
            "request": request,
            "stations": stations,
            "form_error": f"Unknown station_id '{station_id}'",
        }

    work_orders = storage.get_work_orders(active_only=True)
    activities = storage.get_activities_by_station(station_id)
    operators = storage.get_operators()
    precedence_activities = _precedence_activities()

    if values.get("activity_id") == "" and len(activities) == 1:
        values["activity_id"] = activities[0]["activity_id"]

    if values.get("operator_id") == "" and operators:
        values["operator_id"] = operators[0]["operator_id"]

    return {
        "request": request,
        "station": station,
        "work_orders": work_orders,
        "activities": activities,
        "operators": operators,
        "values": values,
        "errors": errors or {},
        "form_error": form_error,
        "reason_codes": REASON_CODES,
        "precedence_activities": precedence_activities,
    }


def _default_form_values(station_id: str, wo_id: str = "") -> dict[str, Any]:
    now = _now_dubai()
    return {
        "supervisor": "",
        "station_id": station_id,
        "wo_id": wo_id,
        "activity_id": "",
        "activity_description": "",
        "activity_description_other": "",
        "operator_id": "",
        "start_time": _format_dt_local(now - timedelta(minutes=START_TIME_OFFSET_MINUTES)),
        "end_time": _format_dt_local(now),
        "qty_good": 0,
        "qty_rework": 0,
        "qty_reject": 0,
        "num_operators": 1,
        "reason_code": "",
        "remarks": "",
        "supervisor_checkin_time": now.isoformat(),
    }


def _coerce_log_payload_from_values(values: dict[str, Any]) -> tuple[dict[str, Any], dict[str, list[str]]]:
    errors: dict[str, list[str]] = {}

    def parse_non_negative_int(raw: str, field: str) -> int:
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            errors.setdefault(field, []).append("Must be an integer")
            return 0
        if parsed < 0:
            errors.setdefault(field, []).append("Must be >= 0")
            return 0
        return parsed

    def parse_positive_int(raw: str, field: str) -> int:
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            errors.setdefault(field, []).append("Must be an integer")
            return 1
        if parsed < 1:
            errors.setdefault(field, []).append("Must be >= 1")
            return 1
        return parsed

    parsed_start = None
    parsed_end = None
    parsed_checkin = None

    try:
        parsed_start = _parse_datetime_input(values.get("start_time", ""))
    except ValueError as exc:
        errors.setdefault("start_time", []).append(str(exc))

    try:
        parsed_end = _parse_datetime_input(values.get("end_time", ""))
    except ValueError as exc:
        errors.setdefault("end_time", []).append(str(exc))

    try:
        parsed_checkin = _parse_datetime_input(values.get("supervisor_checkin_time", ""))
    except ValueError as exc:
        errors.setdefault("supervisor_checkin_time", []).append(str(exc))

    payload_data = {
        "supervisor": values.get("supervisor", ""),
        "station_id": values.get("station_id", ""),
        "wo_id": values.get("wo_id", ""),
        "activity_id": values.get("activity_id", ""),
        "operator_id": values.get("operator_id", ""),
        "start_time": parsed_start,
        "end_time": parsed_end,
        "qty_good": parse_non_negative_int(values.get("qty_good", "0"), "qty_good"),
        "qty_rework": parse_non_negative_int(values.get("qty_rework", "0"), "qty_rework"),
        "qty_reject": parse_non_negative_int(values.get("qty_reject", "0"), "qty_reject"),
        "num_operators": parse_positive_int(values.get("num_operators", "1"), "num_operators"),
        "reason_code": values.get("reason_code", ""),
        "activity_description": values.get("activity_description", ""),
        "remarks": values.get("remarks", ""),
        "supervisor_checkin_time": parsed_checkin,
    }
    return payload_data, errors


def _resolve_quicklog_selection(
    station_id: str,
    selected_activity_id: str = "",
    selected_operator_id: str = "",
) -> dict[str, Any]:
    activities = storage.get_activities_for_station(station_id)
    primary = storage.get_primary_activity_for_station(station_id)
    operators = storage.get_operators()

    if primary is not None:
        activity_id = primary["activity_id"]
        activity_fixed = True
    elif len(activities) == 1:
        activity_id = activities[0]["activity_id"]
        activity_fixed = True
    else:
        activity_id = selected_activity_id or (activities[0]["activity_id"] if activities else "")
        activity_fixed = False

    operator_id = selected_operator_id or (operators[0]["operator_id"] if operators else "")

    selected_activity = next((item for item in activities if item["activity_id"] == activity_id), None)
    return {
        "activities": activities,
        "operators": operators,
        "activity_id": activity_id,
        "operator_id": operator_id,
        "activity_fixed": activity_fixed,
        "selected_activity": selected_activity,
    }


def _build_quicklog_context(
    request: Request,
    station_id: str,
    values: dict[str, Any] | None = None,
    errors: dict[str, list[str]] | None = None,
    form_error: str | None = None,
    success_message: str | None = None,
) -> dict[str, Any]:
    station = storage.get_station(station_id)
    if station is None:
        return {
            "request": request,
            "stations": storage.get_stations(),
            "form_error": f"Unknown station_id '{station_id}'",
        }

    if values is None:
        values = _default_form_values(station_id=station_id)

    selection = _resolve_quicklog_selection(
        station_id=station_id,
        selected_activity_id=values.get("activity_id", ""),
        selected_operator_id=values.get("operator_id", ""),
    )
    values["activity_id"] = selection["activity_id"]
    values["operator_id"] = selection["operator_id"]

    return {
        "request": request,
        "station": station,
        "work_orders": storage.get_work_orders(active_only=True),
        "recent_logs": storage.get_recent_logs_for_station(station_id, limit=10),
        "precedence_activities": _precedence_activities(),
        "values": values,
        "errors": errors or {},
        "form_error": form_error,
        "success_message": success_message,
        "reason_codes": REASON_CODES,
        **selection,
    }


def _work_order_status_options() -> list[str]:
    return ["Not Started", "In Progress", "On Hold", "Completed", "Cancelled"]


def _work_order_plan_catalog() -> tuple[list[dict[str, str]], dict[str, list[dict[str, str]]]]:
    plan_file = BASE_DIR / "data" / "wo_process_plan.csv"
    if not plan_file.exists() or plan_file.stat().st_size == 0:
        return [], {}

    options_by_wo: dict[str, dict[str, str]] = {}
    steps_by_wo: dict[str, list[dict[str, str]]] = {}
    with plan_file.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            wo_number = (row.get("wo_number", "") or row.get("wo_id", "") or "").strip()
            product = (row.get("product", "") or "").strip()
            step_no = (row.get("step_no", "") or "").strip()
            activity_id = (row.get("activity_id", "") or "").strip()
            activity_name = (row.get("activity_name", "") or "").strip()
            if not wo_number:
                continue
            if wo_number not in options_by_wo:
                options_by_wo[wo_number] = {"wo_id": wo_number, "product": product}
            steps_by_wo.setdefault(wo_number, []).append(
                {
                    "step_no": step_no,
                    "activity_id": activity_id,
                    "activity_name": activity_name,
                }
            )

    options = sorted(options_by_wo.values(), key=lambda item: item["wo_id"])
    for wo_number, steps in steps_by_wo.items():
        steps_by_wo[wo_number] = sorted(
            steps,
            key=lambda item: (
                int(item["step_no"]) if item["step_no"].isdigit() else 999999,
                item["step_no"],
                item["activity_id"],
            ),
        )
    return options, steps_by_wo


def _default_work_order_form_values() -> dict[str, Any]:
    now = _now_dubai()
    return {
        "wo_id": "",
        "planned_qty": "1",
        "status": "Not Started",
        "release_time": _format_dt_local(now),
        "due_time": "",
    }


def _parse_dashboard_date(date_text: str) -> date:
    text = (date_text or "").strip()
    if not text:
        return _now_dubai().date()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return _now_dubai().date()


def _parse_positive_int(value: str, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed <= 0:
        return default
    return parsed


def _dashboard_params(date_text: str, window_text: str, stale_text: str) -> tuple[date, int, int, str]:
    target_date = _parse_dashboard_date(date_text)
    window_hours = _parse_positive_int(window_text, DEFAULT_WINDOW_HOURS)
    stale_hours = _parse_positive_int(stale_text, DEFAULT_STALE_HOURS)
    return target_date, window_hours, stale_hours, target_date.isoformat()


def _employee_tracking_data(target_date: date, selected_operator_id: str = "") -> dict[str, Any]:
    operator_rows = storage.get_operators()
    operator_name_map = {row["operator_id"]: row["operator_name"] for row in operator_rows}
    station_name_map = {row["station_id"]: row["station_name"] for row in storage.get_stations()}

    selected_operator = (selected_operator_id or "").strip()
    selected_key = selected_operator.lower()

    per_operator: dict[str, dict[str, Any]] = {}
    recent_logs: list[dict[str, Any]] = []
    totals = {
        "logs_count": 0,
        "qty_good": 0,
        "qty_rework": 0,
        "qty_reject": 0,
        "processing_minutes": 0.0,
    }

    for log in iter_operation_logs():
        ts = parse_dt(log.get("timestamp_created", ""))
        if ts is None or ts.date() != target_date:
            continue

        operator_id = (log.get("operator_id", "") or "").strip()
        if not operator_id:
            continue

        if selected_key and operator_id.lower() != selected_key:
            continue

        operator_name = operator_name_map.get(operator_id, operator_id)
        station_id = (log.get("station_id", "") or "").strip()
        station_name = station_name_map.get(station_id, station_id)

        bucket = per_operator.setdefault(
            operator_id,
            {
                "operator_id": operator_id,
                "operator_name": operator_name,
                "logs_count": 0,
                "qty_good": 0,
                "qty_rework": 0,
                "qty_reject": 0,
                "processing_minutes": 0.0,
                "_wo_ids": set(),
                "_station_ids": set(),
            },
        )

        qty_good = max(0, to_int(log.get("qty_good", "0")))
        qty_rework = max(0, to_int(log.get("qty_rework", "0")))
        qty_reject = max(0, to_int(log.get("qty_reject", "0")))

        bucket["logs_count"] += 1
        bucket["qty_good"] += qty_good
        bucket["qty_rework"] += qty_rework
        bucket["qty_reject"] += qty_reject
        bucket["_wo_ids"].add((log.get("wo_id", "") or "").strip())
        if station_id:
            bucket["_station_ids"].add(station_id)

        totals["logs_count"] += 1
        totals["qty_good"] += qty_good
        totals["qty_rework"] += qty_rework
        totals["qty_reject"] += qty_reject

        start_dt = parse_dt(log.get("start_time", ""))
        end_dt = parse_dt(log.get("end_time", ""))
        processing_minutes = 0.0
        if start_dt is not None and end_dt is not None and end_dt >= start_dt:
            processing_minutes = (end_dt - start_dt).total_seconds() / 60.0
            bucket["processing_minutes"] += processing_minutes
            totals["processing_minutes"] += processing_minutes

        recent_logs.append(
            {
                "timestamp_created": log.get("timestamp_created", ""),
                "wo_id": log.get("wo_id", ""),
                "station_id": station_id,
                "station_name": station_name,
                "activity_id": log.get("activity_id", ""),
                "operator_id": operator_id,
                "operator_name": operator_name,
                "qty_good": qty_good,
                "qty_rework": qty_rework,
                "qty_reject": qty_reject,
                "processing_minutes": processing_minutes,
            }
        )

    rows = []
    for bucket in per_operator.values():
        rows.append(
            {
                "operator_id": bucket["operator_id"],
                "operator_name": bucket["operator_name"],
                "logs_count": bucket["logs_count"],
                "qty_good": bucket["qty_good"],
                "qty_rework": bucket["qty_rework"],
                "qty_reject": bucket["qty_reject"],
                "processing_minutes": bucket["processing_minutes"],
                "wo_count": len([wo_id for wo_id in bucket["_wo_ids"] if wo_id]),
                "station_count": len(bucket["_station_ids"]),
            }
        )

    rows.sort(key=lambda row: row["operator_id"])
    recent_logs.sort(
        key=lambda row: parse_dt(row.get("timestamp_created", "")) or datetime.min.replace(tzinfo=TIMEZONE),
        reverse=True,
    )

    return {
        "date": target_date.isoformat(),
        "selected_operator_id": selected_operator,
        "operators": operator_rows,
        "rows": rows,
        "recent_logs": recent_logs[:25],
        "totals": totals,
    }


@app.get("/")
def home(request: Request) -> HTMLResponse:
    counts = storage.get_counts()
    recent_logs = storage.get_recent_logs(limit=10)
    stations = storage.get_stations()
    work_orders = storage.get_work_orders(active_only=False)
    today = _now_dubai().date()
    station_metrics = compute_all_stations_metrics(
        target_date=today,
        window_hours=DEFAULT_WINDOW_HOURS,
        stale_hours=DEFAULT_STALE_HOURS,
        stations=stations,
        work_orders=work_orders,
    )
    top_bottlenecks = compute_bottlenecks(station_metrics, metric="processing")[:3]
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            **counts,
            "recent_logs": recent_logs,
            "top_bottlenecks": top_bottlenecks,
            "today_date": today.isoformat(),
        },
    )


@app.get("/work-orders")
def work_orders_list(
    request: Request,
    q: str = Query(default=""),
    status: str = Query(default=""),
    view: str = Query(default="active"),
) -> HTMLResponse:
    active_only = view != "all"
    status_filter = status or None
    rows = storage.get_work_orders(active_only=active_only, q=q or None, status=status_filter)
    return templates.TemplateResponse(
        "work_orders.html",
        {
            "request": request,
            "work_orders": rows,
            "q": q,
            "status": status,
            "view": "all" if view == "all" else "active",
            "status_options": _work_order_status_options(),
        },
    )


@app.get("/work-orders/new")
def work_order_new(request: Request) -> HTMLResponse:
    wo_plan_options, wo_plan_steps = _work_order_plan_catalog()
    return templates.TemplateResponse(
        "work_order_new.html",
        {
            "request": request,
            "values": _default_work_order_form_values(),
            "errors": {},
            "form_error": None,
            "status_options": _work_order_status_options(),
            "wo_plan_options": wo_plan_options,
            "wo_plan_steps": wo_plan_steps,
        },
    )


@app.post("/work-orders/new")
def work_order_create(
    request: Request,
    wo_id: str = Form(default=""),
    planned_qty: str = Form(default="1"),
    status: str = Form(default="Not Started"),
    release_time: str = Form(default=""),
    due_time: str = Form(default=""),
) -> HTMLResponse:
    wo_plan_options, wo_plan_steps = _work_order_plan_catalog()
    product_by_wo = {item["wo_id"]: item["product"] for item in wo_plan_options}
    product = product_by_wo.get(wo_id.strip(), "")

    values = {
        "wo_id": wo_id,
        "planned_qty": planned_qty,
        "status": status,
        "release_time": release_time,
        "due_time": due_time,
    }
    errors: dict[str, list[str]] = {}

    try:
        planned_qty_int = int(planned_qty)
    except (TypeError, ValueError):
        errors.setdefault("planned_qty", []).append("Must be an integer")
        planned_qty_int = 0

    try:
        parsed_release = _parse_optional_datetime_input(release_time)
    except ValueError as exc:
        errors.setdefault("release_time", []).append(str(exc))
        parsed_release = None

    try:
        parsed_due = _parse_optional_datetime_input(due_time)
    except ValueError as exc:
        errors.setdefault("due_time", []).append(str(exc))
        parsed_due = None

    if not wo_plan_options:
        errors.setdefault("__all__", []).append("WO plan master is missing: data/wo_process_plan.csv")

    if wo_id.strip() not in product_by_wo:
        errors.setdefault("wo_id", []).append("Select a valid WO Number from dropdown")

    payload_data = {
        "wo_id": wo_id.strip(),
        "product": product,
        "planned_qty": planned_qty_int,
        "status": status,
        "release_time": parsed_release,
        "due_time": parsed_due,
    }

    if errors:
        return templates.TemplateResponse(
            "work_order_new.html",
            {
                "request": request,
                "values": values,
                "errors": errors,
                "form_error": None,
                "status_options": _work_order_status_options(),
                "wo_plan_options": wo_plan_options,
                "wo_plan_steps": wo_plan_steps,
            },
            status_code=422,
        )

    try:
        wo = WorkOrderCreate.model_validate(payload_data)
    except ValidationError as exc:
        return templates.TemplateResponse(
            "work_order_new.html",
            {
                "request": request,
                "values": values,
                "errors": _errors_by_field(exc),
                "form_error": None,
                "status_options": _work_order_status_options(),
                "wo_plan_options": wo_plan_options,
                "wo_plan_steps": wo_plan_steps,
            },
            status_code=422,
        )

    if storage.work_order_exists(wo.wo_id):
        return templates.TemplateResponse(
            "work_order_new.html",
            {
                "request": request,
                "values": values,
                "errors": {"wo_id": ["WO Number already exists"]},
                "form_error": None,
                "status_options": _work_order_status_options(),
                "wo_plan_options": wo_plan_options,
                "wo_plan_steps": wo_plan_steps,
            },
            status_code=409,
        )

    try:
        storage.append_work_order(
            {
                "wo_id": wo.wo_id,
                "product": wo.product,
                "planned_qty": wo.planned_qty,
                "status": wo.status,
                "release_time": wo.release_time,
                "due_time": wo.due_time,
                "created_at": _now_dubai(),
            }
        )
    except StorageError as exc:
        return templates.TemplateResponse(
            "work_order_new.html",
            {
                "request": request,
                "values": values,
                "errors": {},
                "form_error": str(exc),
                "status_options": _work_order_status_options(),
                "wo_plan_options": wo_plan_options,
                "wo_plan_steps": wo_plan_steps,
            },
            status_code=422,
        )

    return RedirectResponse(url=f"/work-orders/{wo.wo_id}", status_code=303)


@app.get("/work-orders/{wo_id}")
def work_order_detail(request: Request, wo_id: str) -> HTMLResponse:
    wo = storage.get_work_order(wo_id)
    if wo is None:
        raise HTTPException(status_code=404, detail="Work order not found")

    logs = storage.get_logs_for_wo(wo["wo_id"])
    metrics = storage.compute_wo_metrics(wo, logs)
    activities = {row["activity_id"]: row["activity_name"] for row in storage.read_master_table("activities")}

    log_rows = []
    for row in logs:
        start_dt = _safe_parse_datetime(row.get("start_time", ""))
        end_dt = _safe_parse_datetime(row.get("end_time", ""))
        duration_seconds = None
        if start_dt is not None and end_dt is not None and end_dt >= start_dt:
            duration_seconds = (end_dt - start_dt).total_seconds()
        log_rows.append(
            {
                **row,
                "duration_display": _format_seconds_human(duration_seconds),
            }
        )

    bottlenecks = [
        {
            "activity_id": activity_id,
            "activity_name": activities.get(activity_id, activity_id),
            "duration_display": _format_seconds_human(seconds),
        }
        for activity_id, seconds in metrics["top_bottlenecks"]
    ]

    return templates.TemplateResponse(
        "work_order_detail.html",
        {
            "request": request,
            "wo": wo,
            "metrics": {
                **metrics,
                "first_start_display": _format_dt_display(metrics["first_start"]),
                "last_end_display": _format_dt_display(metrics["last_end"]),
                "processing_display": _format_seconds_human(metrics["processing_seconds"]),
                "lead_time_display": _format_seconds_human(metrics["lead_time_seconds"]),
            },
            "bottlenecks": bottlenecks,
            "log_rows": log_rows,
        },
    )


@app.get("/dashboard/stations")
def dashboard_stations(
    request: Request,
    date: str = Query(default=""),
    window_hours: str = Query(default=str(DEFAULT_WINDOW_HOURS)),
    stale_hours: str = Query(default=str(DEFAULT_STALE_HOURS)),
) -> HTMLResponse:
    target_date, win_hours, stale_hrs, date_str = _dashboard_params(date, window_hours, stale_hours)
    stations = storage.get_stations()
    work_orders = storage.get_work_orders(active_only=False)
    metrics = compute_all_stations_metrics(
        target_date=target_date,
        window_hours=win_hours,
        stale_hours=stale_hrs,
        stations=stations,
        work_orders=work_orders,
    )
    return templates.TemplateResponse(
        "dashboard_stations.html",
        {
            "request": request,
            "date": date_str,
            "window_hours": win_hours,
            "stale_hours": stale_hrs,
            "metrics": metrics,
        },
    )


@app.get("/dashboard/stations/{station_id}")
def dashboard_station_detail(
    request: Request,
    station_id: str,
    date: str = Query(default=""),
    window_hours: str = Query(default=str(DEFAULT_WINDOW_HOURS)),
    stale_hours: str = Query(default=str(DEFAULT_STALE_HOURS)),
) -> HTMLResponse:
    target_date, win_hours, stale_hrs, date_str = _dashboard_params(date, window_hours, stale_hours)
    station = storage.get_station(station_id)
    if station is None:
        raise HTTPException(status_code=404, detail="Station not found")

    stations = storage.get_stations()
    work_orders = storage.get_work_orders(active_only=False)
    metrics = compute_station_metrics(
        station_id=station_id,
        target_date=target_date,
        window_hours=win_hours,
        stale_hours=stale_hrs,
        stations=stations,
        work_orders=work_orders,
    )
    detail_tables = compute_station_detail_tables(
        station_id=station_id,
        target_date=target_date,
        window_hours=win_hours,
        stale_hours=stale_hrs,
        work_orders=work_orders,
    )

    active_wos = []
    for row in detail_tables["active_wos"]:
        active_wos.append(
            {
                **row,
                "last_update_display": _format_dt_display(row["last_update_time"]),
            }
        )

    return templates.TemplateResponse(
        "dashboard_station_detail.html",
        {
            "request": request,
            "station": station,
            "date": date_str,
            "window_hours": win_hours,
            "stale_hours": stale_hrs,
            "metrics": metrics,
            "active_wos": active_wos,
            "recent_logs": detail_tables["recent_logs"],
            "reasons": detail_tables["reasons"],
        },
    )


@app.get("/dashboard/bottlenecks")
def dashboard_bottlenecks(
    request: Request,
    date: str = Query(default=""),
    metric: str = Query(default="processing"),
    window_hours: str = Query(default=str(DEFAULT_WINDOW_HOURS)),
    stale_hours: str = Query(default=str(DEFAULT_STALE_HOURS)),
) -> HTMLResponse:
    target_date, win_hours, stale_hrs, date_str = _dashboard_params(date, window_hours, stale_hours)
    stations = storage.get_stations()
    work_orders = storage.get_work_orders(active_only=False)
    metrics = compute_all_stations_metrics(
        target_date=target_date,
        window_hours=win_hours,
        stale_hours=stale_hrs,
        stations=stations,
        work_orders=work_orders,
    )
    selected_metric = metric if metric in {"processing", "wip", "stale", "rejects"} else "processing"
    ranked = compute_bottlenecks(metrics, selected_metric)
    explanations = {
        "processing": "Ranks by total reported processing minutes from valid start/end pairs on selected date.",
        "wip": "Ranks by number of active WOs with latest station update inside the selected window.",
        "stale": "Ranks by active WOs whose latest station update is older than stale threshold.",
        "rejects": "Ranks by total rejected quantity logged on selected date.",
    }
    return templates.TemplateResponse(
        "dashboard_bottlenecks.html",
        {
            "request": request,
            "date": date_str,
            "window_hours": win_hours,
            "stale_hours": stale_hrs,
            "metric": selected_metric,
            "ranked": ranked,
            "explanation": explanations[selected_metric],
        },
    )


@app.get("/tracking/employees")
def employee_tracking(
    request: Request,
    date: str = Query(default=""),
    operator_id: str = Query(default=""),
) -> HTMLResponse:
    target_date = _parse_dashboard_date(date)
    context = _employee_tracking_data(target_date=target_date, selected_operator_id=operator_id)
    return templates.TemplateResponse(
        "employee_tracking.html",
        {
            "request": request,
            **context,
        },
    )


@app.get("/exports/daily")
def export_daily(date: str = Query(default="")) -> Response:
    target_date = _parse_dashboard_date(date)
    stations = storage.get_stations()
    work_orders = storage.get_work_orders(active_only=False)
    metrics = compute_all_stations_metrics(
        target_date=target_date,
        window_hours=DEFAULT_WINDOW_HOURS,
        stale_hours=DEFAULT_STALE_HOURS,
        stations=stations,
        work_orders=work_orders,
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "station_id",
            "station_name",
            "logs_count",
            "qty_good",
            "qty_rework",
            "qty_reject",
            "total_processing_minutes",
            "avg_cycle_minutes",
            "wip_count",
            "stale_wip_count",
        ]
    )
    for row in metrics:
        writer.writerow(
            [
                row["station_id"],
                row["station_name"],
                row["logs_count_today"],
                row["qty_good_today"],
                row["qty_rework_today"],
                row["qty_reject_today"],
                f"{row['total_reported_processing_minutes_today']:.2f}",
                f"{row['avg_cycle_minutes_today']:.2f}",
                row["wip_count"],
                row["stale_wip_count"],
            ]
        )

    csv_text = output.getvalue()
    filename = f"daily_station_summary_{target_date.isoformat()}.csv"
    headers = {"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    return Response(content=csv_text, media_type="text/csv", headers=headers)


@app.get("/api/charts/wo-status")
def api_chart_wo_status(date: str = Query(default="")) -> JSONResponse:
    _ = _parse_dashboard_date(date)
    work_orders = storage.get_work_orders(active_only=False)
    counts = count_work_orders_by_status(work_orders)
    labels = ["Not Started", "In Progress", "On Hold", "Completed", "Cancelled"]
    values = [counts.get(label, 0) for label in labels]
    return JSONResponse(content={"labels": labels, "values": values})


@app.get("/api/charts/rejects-rework")
def api_chart_rejects_rework(date: str = Query(default="")) -> JSONResponse:
    target_date = _parse_dashboard_date(date)
    stations = storage.get_stations()
    rows = aggregate_rejects_rework_by_station(target_date=target_date, stations=stations)
    labels = [f"{row['station_id']} {row['station_name']}" for row in rows]
    rejects = [row["rejects"] for row in rows]
    rework = [row["rework"] for row in rows]
    return JSONResponse(content={"labels": labels, "rejects": rejects, "rework": rework})


@app.get("/api/charts/bottlenecks")
def api_chart_bottlenecks(date: str = Query(default="")) -> JSONResponse:
    target_date = _parse_dashboard_date(date)
    stations = storage.get_stations()
    rows = aggregate_processing_minutes_by_station(target_date=target_date, stations=stations)
    labels = [f"{row['station_id']} {row['station_name']}" for row in rows]
    processing_minutes = [round(float(row["processing_minutes"]), 2) for row in rows]
    return JSONResponse(content={"labels": labels, "processing_minutes": processing_minutes})


@app.get("/stations")
def stations_menu(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "stations.html",
        {"request": request, "stations": storage.get_stations()},
    )


@app.get("/stations/{station_id}/quicklog")
def station_quicklog(request: Request, station_id: str) -> HTMLResponse:
    context = _build_quicklog_context(request=request, station_id=station_id)
    if "station" not in context:
        return templates.TemplateResponse("station_select.html", context, status_code=404)
    return templates.TemplateResponse("station_quicklog.html", context)


@app.post("/stations/{station_id}/quicklog")
def station_quicklog_submit(
    request: Request,
    station_id: str,
    supervisor: str = Form(default=""),
    wo_id: str = Form(default=""),
    activity_id: str = Form(default=""),
    activity_description: str = Form(default=""),
    activity_description_other: str = Form(default=""),
    operator_id: str = Form(default=""),
    start_time: str = Form(default=""),
    end_time: str = Form(default=""),
    qty_good: str = Form(default="0"),
    qty_rework: str = Form(default="0"),
    qty_reject: str = Form(default="0"),
    num_operators: str = Form(default="1"),
    reason_code: str = Form(default=""),
    remarks: str = Form(default=""),
    supervisor_checkin_time: str = Form(default=""),
) -> HTMLResponse:
    values = {
        "supervisor": supervisor,
        "station_id": station_id,
        "wo_id": wo_id,
        "activity_id": activity_id,
        "activity_description": activity_description,
        "activity_description_other": activity_description_other,
        "operator_id": operator_id,
        "start_time": start_time,
        "end_time": end_time,
        "qty_good": qty_good,
        "qty_rework": qty_rework,
        "qty_reject": qty_reject,
        "num_operators": num_operators,
        "reason_code": reason_code,
        "remarks": remarks,
        "supervisor_checkin_time": supervisor_checkin_time,
    }

    selection = _resolve_quicklog_selection(station_id, activity_id, operator_id)
    values["activity_id"] = selection["activity_id"]
    values["operator_id"] = selection["operator_id"]

    payload_data, errors = _coerce_log_payload_from_values(values)
    selected_description = (activity_description or "").strip()
    selected_description_other = (activity_description_other or "").strip()
    if selected_description == "__other__":
        if not selected_description_other:
            errors.setdefault("activity_description_other", []).append("Please enter activity description")
        else:
            selected_description = selected_description_other
    if selected_description and selected_description != "__other__":
        payload_data["activity_description"] = selected_description

    if errors:
        context = _build_quicklog_context(
            request=request,
            station_id=station_id,
            values=values,
            errors=errors,
        )
        if "station" not in context:
            return templates.TemplateResponse("station_select.html", context, status_code=404)
        return templates.TemplateResponse("station_quicklog.html", context, status_code=422)

    try:
        payload = OperationLogCreate.model_validate(payload_data)
    except ValidationError as exc:
        context = _build_quicklog_context(
            request=request,
            station_id=station_id,
            values=values,
            errors=_errors_by_field(exc),
        )
        if "station" not in context:
            return templates.TemplateResponse("station_select.html", context, status_code=404)
        return templates.TemplateResponse("station_quicklog.html", context, status_code=422)

    try:
        storage.append_operation_log(payload.model_dump())
    except StorageError as exc:
        context = _build_quicklog_context(
            request=request,
            station_id=station_id,
            values=values,
            form_error=str(exc),
        )
        if "station" not in context:
            return templates.TemplateResponse("station_select.html", context, status_code=404)
        return templates.TemplateResponse("station_quicklog.html", context, status_code=422)

    refreshed_defaults = _default_form_values(station_id=station_id)
    context = _build_quicklog_context(
        request=request,
        station_id=station_id,
        values=refreshed_defaults,
        success_message="Saved",
    )
    return templates.TemplateResponse("station_quicklog.html", context)


@app.get("/logs/new")
def new_log_form(
    request: Request,
    station_id: str = Query(default=""),
    wo_id: str = Query(default=""),
) -> HTMLResponse:
    if station_id:
        return RedirectResponse(url=f"/stations/{station_id}/quicklog", status_code=303)
    return RedirectResponse(url="/stations", status_code=303)


@app.post("/logs/new")
def create_log(
    request: Request,
    supervisor: str = Form(default=""),
    station_id: str = Form(default=""),
    wo_id: str = Form(default=""),
    activity_id: str = Form(default=""),
    activity_description: str = Form(default=""),
    activity_description_other: str = Form(default=""),
    operator_id: str = Form(default=""),
    start_time: str = Form(default=""),
    end_time: str = Form(default=""),
    qty_good: str = Form(default="0"),
    qty_rework: str = Form(default="0"),
    qty_reject: str = Form(default="0"),
    num_operators: str = Form(default="1"),
    reason_code: str = Form(default=""),
    remarks: str = Form(default=""),
    supervisor_checkin_time: str = Form(default=""),
) -> HTMLResponse:
    target_station_id = (station_id or "").strip()
    if target_station_id:
        return RedirectResponse(url=f"/stations/{target_station_id}/quicklog", status_code=303)
    return RedirectResponse(url="/stations", status_code=303)


@app.get("/api/options")
def api_options(station_id: str = Query(default="")) -> JSONResponse:
    activities = storage.read_master_table("activities")
    operators = storage.read_master_table("operators")
    if station_id:
        activities = [item for item in activities if item["station_id"] == station_id]
        operators = [item for item in operators if item["station_id"] == station_id]

    data = {
        "stations": storage.get_stations(),
        "work_orders": storage.get_work_orders(active_only=True),
        "activities": sorted(activities, key=lambda item: item["activity_id"]),
        "operators": sorted(operators, key=lambda item: item["operator_id"]),
    }
    return JSONResponse(content=jsonable_encoder(data))
