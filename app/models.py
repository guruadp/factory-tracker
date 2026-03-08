from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StationRow(BaseModel):
    station_id: str = Field(..., min_length=1)
    station_name: str = Field(..., min_length=1)
    description: str = ""


class ActivityRow(BaseModel):
    activity_id: str = Field(..., min_length=1)
    activity_name: str = Field(..., min_length=1)
    station_id: str = Field(..., min_length=1)
    is_primary: bool = False


class OperatorRow(BaseModel):
    operator_id: str = Field(..., min_length=1)
    operator_name: str = Field(..., min_length=1)
    station_id: str = Field(..., min_length=1)


class WorkOrderRow(BaseModel):
    wo_id: str = Field(..., min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    product: str = Field(..., min_length=1)
    planned_qty: int = Field(..., ge=1)
    status: Literal["Not Started", "In Progress", "On Hold", "Completed", "Cancelled"]
    release_time: Optional[datetime] = None
    due_time: Optional[datetime] = None
    process_stations: str = ""
    created_at: datetime

    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("release_time", "due_time", mode="before")
    @classmethod
    def empty_string_to_none(cls, value):
        if value == "":
            return None
        return value

    @model_validator(mode="after")
    def validate_due_vs_release(self) -> "WorkOrderRow":
        if self.release_time is not None and self.due_time is not None and self.due_time < self.release_time:
            raise ValueError("due_time must be greater than or equal to release_time")
        return self


class WorkOrderCreate(BaseModel):
    wo_id: str = Field(..., min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    product: str = Field(..., min_length=1)
    planned_qty: int = Field(..., ge=1)
    status: Literal["Not Started", "In Progress", "On Hold", "Completed", "Cancelled"] = "Not Started"
    release_time: Optional[datetime] = None
    due_time: Optional[datetime] = None
    process_stations: str = ""

    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("release_time", "due_time", mode="before")
    @classmethod
    def empty_string_to_none_create(cls, value):
        if value == "":
            return None
        return value

    @field_validator("release_time", "due_time")
    @classmethod
    def require_timezone_when_present(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return value
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("datetime must include timezone information")
        return value

    @model_validator(mode="after")
    def validate_due_vs_release(self) -> "WorkOrderCreate":
        if self.release_time is not None and self.due_time is not None and self.due_time < self.release_time:
            raise ValueError("due_time must be greater than or equal to release_time")
        return self


class OperationLogCreate(BaseModel):
    supervisor: str = Field(..., min_length=1)
    station_id: str = Field(..., min_length=1)
    wo_id: str = Field(..., min_length=1)
    activity_id: str = Field(..., min_length=1)
    operator_id: str = ""
    start_time: datetime
    end_time: datetime
    qty_good: int = Field(..., ge=0)
    qty_rework: int = Field(..., ge=0)
    qty_reject: int = Field(..., ge=0)
    num_operators: int = Field(..., ge=1)
    reason_code: str = ""
    activity_description: str = ""
    remarks: str = ""
    supervisor_checkin_time: datetime
    supervisor_checkout_time: Optional[datetime] = None

    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("start_time", "end_time", "supervisor_checkin_time", "supervisor_checkout_time")
    @classmethod
    def require_timezone_aware(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return value
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("datetime must include timezone information")
        return value

    @model_validator(mode="after")
    def validate_log_values(self) -> "OperationLogCreate":
        if self.end_time < self.start_time:
            raise ValueError("end_time must be greater than or equal to start_time")

        total_qty = self.qty_good + self.qty_rework + self.qty_reject
        if total_qty <= 0:
            raise ValueError("qty_good + qty_rework + qty_reject must be greater than 0")

        if (
            self.supervisor_checkout_time is not None
            and self.supervisor_checkout_time < self.supervisor_checkin_time
        ):
            raise ValueError(
                "supervisor_checkout_time must be greater than or equal to supervisor_checkin_time"
            )

        return self


class OperationLogRow(BaseModel):
    log_id: str = Field(..., min_length=1)
    timestamp_created: datetime
    supervisor: str
    station_id: str
    wo_id: str
    activity_id: str
    operator_id: str
    start_time: datetime
    end_time: datetime
    qty_good: int
    qty_rework: int
    qty_reject: int
    num_operators: int
    reason_code: str = ""
    activity_description: str = ""
    remarks: str = ""
    supervisor_checkin_time: datetime
    supervisor_checkout_time: datetime

    model_config = ConfigDict(str_strip_whitespace=True)
