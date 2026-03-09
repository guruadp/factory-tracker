from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import DATABASE_URL
from app.db_models import Base

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def _normalized_database_url() -> str:
    url = DATABASE_URL
    if not url:
        return ""
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://") :]
    if url.startswith("postgresql://") and "+psycopg" not in url:
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def is_database_enabled() -> bool:
    return bool(_normalized_database_url())


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = _normalized_database_url()
        if not url:
            raise RuntimeError("DATABASE_URL is not configured")
        _engine = create_engine(url, pool_pre_ping=True, future=True)
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True)
    return _session_factory


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ensure_database_schema() -> None:
    Base.metadata.create_all(get_engine())
