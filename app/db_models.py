from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Station(Base):
    __tablename__ = "stations"

    station_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    station_name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")


class Activity(Base):
    __tablename__ = "activities"

    activity_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    activity_name: Mapped[str] = mapped_column(String(255), nullable=False)
    station_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class Operator(Base):
    __tablename__ = "operators"

    operator_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    operator_name: Mapped[str] = mapped_column(String(255), nullable=False)
    station_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)


class WorkOrder(Base):
    __tablename__ = "work_orders"

    wo_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    product: Mapped[str] = mapped_column(String(255), nullable=False)
    planned_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False)
    release_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    due_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    process_stations: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OperationLog(Base):
    __tablename__ = "operation_logs"

    log_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    timestamp_created: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    supervisor: Mapped[str] = mapped_column(String(255), nullable=False)
    station_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    wo_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    activity_id: Mapped[str] = mapped_column(String(64), nullable=False)
    operator_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    qty_good: Mapped[int] = mapped_column(Integer, nullable=False)
    qty_rework: Mapped[int] = mapped_column(Integer, nullable=False)
    qty_reject: Mapped[int] = mapped_column(Integer, nullable=False)
    num_operators: Mapped[int] = mapped_column(Integer, nullable=False)
    reason_code: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    activity_description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    remarks: Mapped[str] = mapped_column(Text, nullable=False, default="")
    supervisor_checkin_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    supervisor_checkout_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
