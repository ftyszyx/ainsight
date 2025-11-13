from __future__ import annotations

import argparse
import logging
from datetime import date

import pandas as pd

from sqlalchemy import text

from market.services.db import get_engine
from market.services.storage import save_dataframe
from market.services.tushare_client import TushareClient

logger = logging.getLogger("jobs.fetch_daily")


def _validate_date(value: str) -> str:
    if len(value) != 8 or not value.isdigit():
        raise argparse.ArgumentTypeError("Date must be in YYYYMMDD format.")
    return value


def _default_trade_date() -> str:
    today = date.today()
    return today.strftime("%Y%m%d")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch daily A-share market data.")
    parser.add_argument("--date", type=_validate_date, default=_default_trade_date(), help="Trade date in YYYYMMDD.")
    parser.add_argument("--replace", action="store_true", help="Replace records for the same date before insert.")
    return parser.parse_args()


def load_dataframe(df: pd.DataFrame, trade_date: str, replace: bool) -> None:
    if df.empty:
        logger.warning("No data returned for %s.", trade_date)
        return
    df = df.rename(columns={"ts_code": "security_id", "vol": "volume"})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    engine = get_engine()
    with engine.begin() as connection:
        if replace:
            connection.execute(
                text("DELETE FROM daily_prices WHERE trade_date = :trade_date"),
                {"trade_date": df["trade_date"].iloc[0]},
            )
        df.to_sql("daily_prices", connection, if_exists="append", index=False, method="multi", chunksize=1000)
    logger.info("Inserted %d rows into daily_prices.", len(df))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    client = TushareClient()
    logger.info("Fetching daily data for %s.", args.date)
    df = client.daily(args.date)
    save_dataframe(df, "daily", args.date, f"daily_{args.date}.csv")
    load_dataframe(df, args.date, args.replace)
    logger.info("Daily data pipeline completed for %s.", args.date)


if __name__ == "__main__":
    main()

