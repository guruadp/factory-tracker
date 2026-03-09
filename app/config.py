import os
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

TZ_NAME = "Asia/Dubai"
TIMEZONE = ZoneInfo(TZ_NAME)
START_TIME_OFFSET_MINUTES = 30
DEFAULT_WINDOW_HOURS = 24
DEFAULT_STALE_HOURS = 4
REASON_CODES = (
    "",
    "Waiting Material",
    "Machine Down",
    "Quality Issue",
    "Rework",
    "Operator Absent",
    "Changeover",
    "Other",
)

STATIONS_FILE = DATA_DIR / "stations.csv"
ACTIVITIES_FILE = DATA_DIR / "activities.csv"
OPERATORS_FILE = DATA_DIR / "operators.csv"
WORK_ORDERS_FILE = DATA_DIR / "work_orders.csv"
OPERATION_LOGS_FILE = DATA_DIR / "operation_logs.csv"
OPERATION_LOGS_LOCK_FILE = DATA_DIR / "operation_logs.csv.lock"
DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()

MASTER_TABLE_FILES = {
    "stations": STATIONS_FILE,
    "activities": ACTIVITIES_FILE,
    "operators": OPERATORS_FILE,
    "work_orders": WORK_ORDERS_FILE,
}
