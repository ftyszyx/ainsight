from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_url: str
    tushare_token: str
    llm_endpoint: Optional[str]
    llm_api_key: Optional[str]
    data_root: Path


def _resolve_data_root() -> Path:
    data_dir = os.getenv("DATA_DIR", "data")
    path = Path(data_dir).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    (path / "raw").mkdir(parents=True, exist_ok=True)
    (path / "logs").mkdir(parents=True, exist_ok=True)
    return path


_cached_settings: Optional[Settings] = None


def get_settings() -> Settings:
    global _cached_settings
    if _cached_settings is None:
        load_dotenv()
        database_url = os.getenv("DATABASE_URL")
        tushare_token = os.getenv("TUSHARE_TOKEN")
        if not database_url:
            raise RuntimeError("DATABASE_URL is not set; please update your .env file.")
        if not tushare_token:
            raise RuntimeError("TUSHARE_TOKEN is not set; please update your .env file.")
        _cached_settings = Settings(
            database_url=database_url,
            tushare_token=tushare_token,
            llm_endpoint=os.getenv("LLM_ENDPOINT"),
            llm_api_key=os.getenv("LLM_API_KEY"),
            data_root=_resolve_data_root(),
        )
    return _cached_settings

