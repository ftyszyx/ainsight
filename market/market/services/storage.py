from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pandas import DataFrame

from market.config import get_settings


def _raw_root() -> Path:
    return get_settings().data_root / "raw"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def raw_dir(category: str, date_str: str) -> Path:
    base = _raw_root() / category / date_str
    return ensure_dir(base)


def save_dataframe(df: DataFrame, category: str, date_str: str, filename: str) -> Path:
    directory = raw_dir(category, date_str)
    path = directory / filename
    df.to_csv(path, index=False)
    return path


def save_lines(lines: Iterable[str], category: str, date_str: str, filename: str) -> Path:
    directory = raw_dir(category, date_str)
    path = directory / filename
    with path.open("w", encoding="utf-8") as file_obj:
        for line in lines:
            file_obj.write(line)
    return path

