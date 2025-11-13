from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from market.services.db import get_engine
from market.services.storage import save_dataframe

logger = logging.getLogger("jobs.calc_features")


def _validate_date(value: str) -> str:
    if len(value) != 8 or not value.isdigit():
        raise argparse.ArgumentTypeError("Date must be YYYYMMDD.")
    return value


def _default_date() -> str:
    return date.today().strftime("%Y%m%d")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calculate factor features for A-share universe.")
    parser.add_argument("--date", type=_validate_date, default=_default_date(), help="Trade date in YYYYMMDD.")
    parser.add_argument("--window", type=int, default=60, help="Number of historical days to include.")
    parser.add_argument("--replace", action="store_true", help="Replace existing snapshot for the date.")
    return parser.parse_args()


def fetch_price_history(trade_date: str, window: int) -> pd.DataFrame:
    engine = get_engine()
    end_date = pd.to_datetime(trade_date)
    start_date = end_date - timedelta(days=window * 2)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT security_id, trade_date, close, volume
                FROM daily_prices
                WHERE trade_date BETWEEN :start_date AND :end_date
                ORDER BY security_id, trade_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def compute_price_features(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if df.empty:
        return df
    pivot = df.pivot(index="trade_date", columns="security_id", values="close")
    target_date = pd.to_datetime(trade_date)
    if target_date not in pivot.index:
        logger.warning("Trade date %s not present in daily_prices.", trade_date)
        return pd.DataFrame()
    returns = pivot.pct_change()
    rolling_5 = returns.rolling(5).sum()
    rolling_20 = returns.rolling(20).sum()
    vol = df.pivot(index="trade_date", columns="security_id", values="volume")
    vol_mean_5 = vol.rolling(5).mean()
    vol_mean_20 = vol.rolling(20).mean()
    close_series = pivot.loc[target_date]
    ret5_series = rolling_5.loc[target_date]
    ret20_series = rolling_20.loc[target_date]
    vol_ratio = (vol_mean_5.loc[target_date] / vol_mean_20.loc[target_date]).replace([pd.NA, float("inf")], None)
    features = pd.DataFrame(
        {
            "security_id": close_series.index,
            "close": close_series.values,
            "ret_5d": ret5_series.values,
            "ret_20d": ret20_series.values,
            "vol_ratio_5_20": vol_ratio.values,
        }
    )
    return features


def fetch_financial_metrics(trade_date: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT DISTINCT ON (security_id)
                    security_id,
                    period_end,
                    roe,
                    netprofit_margin,
                    grossprofit_margin,
                    asset_turn
                FROM financial_metrics
                WHERE period_end <= :trade_date
                ORDER BY security_id, period_end DESC
                """
            ),
            {"trade_date": pd.to_datetime(trade_date)},
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        return df
    numeric_cols = ["roe", "netprofit_margin", "grossprofit_margin", "asset_turn"]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def fetch_sentiment(trade_date: str) -> pd.DataFrame:
    engine = get_engine()
    end_dt = pd.to_datetime(trade_date)
    start_dt = end_dt - timedelta(days=30)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT security_id, AVG(sentiment) AS sentiment_30d
                FROM news
                WHERE publish_time BETWEEN :start AND :end
                GROUP BY security_id
                """
            ),
            {"start": start_dt, "end": end_dt},
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


def combine_features(price_features: pd.DataFrame, financials: pd.DataFrame, sentiment: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if price_features.empty:
        return pd.DataFrame()
    combined = price_features.copy()
    combined["snapshot_date"] = pd.to_datetime(trade_date)
    if not financials.empty:
        combined = combined.merge(financials, on="security_id", how="left")
    if not sentiment.empty:
        combined = combined.merge(sentiment, on="security_id", how="left")
    return combined


def load_snapshot(df: pd.DataFrame, trade_date: str, replace: bool) -> None:
    if df.empty:
        logger.warning("No features computed for %s.", trade_date)
        return
    engine = get_engine()
    with engine.begin() as conn:
        if replace:
            conn.execute(
                text("DELETE FROM feature_snapshots WHERE snapshot_date = :snapshot_date"),
                {"snapshot_date": df["snapshot_date"].iloc[0]},
            )
        df.to_sql("feature_snapshots", conn, if_exists="append", index=False, method="multi", chunksize=1000)
    logger.info("Inserted %d feature rows for %s.", len(df), trade_date)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    logger.info("Calculating features for %s.", args.date)
    price_history = fetch_price_history(args.date, args.window)
    price_features = compute_price_features(price_history, args.date)
    financials = fetch_financial_metrics(args.date)
    sentiment = fetch_sentiment(args.date)
    snapshot = combine_features(price_features, financials, sentiment, args.date)
    save_dataframe(snapshot, "features", args.date, f"features_{args.date}.csv")
    load_snapshot(snapshot, args.date, args.replace)
    logger.info("Feature calculation completed for %s.", args.date)


if __name__ == "__main__":
    main()

