from __future__ import annotations

import time
from typing import Optional

import pandas as pd
import tushare as ts

from market.config import get_settings


class TushareClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = ts.pro_api(settings.tushare_token)

    def daily(self, trade_date: str) -> pd.DataFrame:
        return self._call_with_retry(
            lambda: self._client.daily(
                trade_date=trade_date,
                fields="ts_code,trade_date,open,high,low,close,vol,amount",
            )
        )

    def fina_indicator(self, period: str) -> pd.DataFrame:
        return self._call_with_retry(
            lambda: self._client.fina_indicator(
                period=period,
                fields="ts_code,end_date,roe,roa,q_dtprofit,q_dtprofit_yoy,grossprofit_margin,netprofit_margin,asset_turn"
            )
        )

    def news(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self._call_with_retry(
            lambda: self._client.news(
                start_date=start_date,
                end_date=end_date,
                src="sina"
            )
        )

    @staticmethod
    def _call_with_retry(func, retry: int = 3, delay: float = 1.0) -> pd.DataFrame:
        last_error: Optional[Exception] = None
        for _ in range(retry):
            try:
                df = func()
                return df if df is not None else pd.DataFrame()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                time.sleep(delay)
        if last_error:
            raise last_error
        return pd.DataFrame()

