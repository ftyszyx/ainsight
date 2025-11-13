from __future__ import annotations

import argparse
import logging

import pandas as pd

from sqlalchemy import text

from market.services.db import get_engine
from market.services.storage import save_dataframe
from market.services.tushare_client import TushareClient

logger = logging.getLogger("jobs.sync_financials")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync quarterly financial indicators.")
    parser.add_argument("--period", required=True, help="Reporting period in YYYYMMDD (e.g. 20231231).")
    parser.add_argument("--replace", action="store_true", help="Replace existing period data before insert.")
    return parser.parse_args()


def transform(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.rename(columns={"ts_code": "security_id", "end_date": "period_end"})
    df["period_end"] = pd.to_datetime(df["period_end"])
    numeric_cols = [
        "roe",
        "roa",
        "q_dtprofit",
        "q_dtprofit_yoy",
        "grossprofit_margin",
        "netprofit_margin",
        "asset_turn",
    ]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def load_dataframe(df: pd.DataFrame, period: str, replace: bool) -> None:
    if df.empty:
        logger.warning("No financial indicator data for %s.", period)
        return
    engine = get_engine()
    with engine.begin() as connection:
        if replace:
            connection.execute(text("DELETE FROM financial_metrics WHERE period_end = :period"), {"period": df["period_end"].iloc[0]})
        df.to_sql("financial_metrics", connection, if_exists="append", index=False, method="multi", chunksize=1000)
    logger.info("Inserted %d rows into financial_metrics.", len(df))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    logger.info("Fetching financial indicators for period %s.", args.period)
    client = TushareClient()
    df = client.fina_indicator(args.period)
    df = transform(df)
    save_dataframe(df, "financials", args.period, f"financials_{args.period}.csv")
    load_dataframe(df, args.period, args.replace)
    logger.info("Financial indicators pipeline completed for %s.", args.period)


if __name__ == "__main__":
    main()

