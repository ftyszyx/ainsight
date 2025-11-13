from __future__ import annotations

from functools import lru_cache
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from market.config import get_settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    settings = get_settings()
    engine = create_engine(settings.database_url, echo=False, pool_pre_ping=True, future=True)
    return engine


def get_session() -> Generator[Session, None, None]:
    factory = sessionmaker(bind=get_engine(), expire_on_commit=False, future=True)
    with factory() as session:
        yield session


def run_healthcheck() -> bool:
    with get_engine().connect() as conn:
        conn.execute(text("SELECT 1"))
    return True

