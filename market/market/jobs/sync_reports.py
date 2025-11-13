from __future__ import annotations

import argparse
import json
import logging
from typing import List

import pandas as pd
from sqlalchemy import text

from market.services.db import get_engine
from market.services.llm import LLMUnavailable, summarize_report
from market.services.storage import save_dataframe
from market.services.tushare_client import TushareClient

logger = logging.getLogger("jobs.sync_reports")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync research reports and news summaries.")
    parser.add_argument("--start-date", required=True, help="Start date in YYYYMMDD format.")
    parser.add_argument("--end-date", required=True, help="End date in YYYYMMDD format.")
    parser.add_argument("--limit", type=int, default=50, help="Maximum number of items to process.")
    parser.add_argument("--replace", action="store_true", help="Delete existing entries in the date window before insert.")
    return parser.parse_args()


def transform(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.head(limit)
    df = df.rename(
        columns={
            "datetime": "publish_time",
            "content": "body",
            "title": "title",
            "url": "source_url",
        }
    )
    df["publish_time"] = pd.to_datetime(df["publish_time"])
    return df


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    summaries: List[str] = []
    sentiments: List[float] = []
    risks: List[str] = []
    highlights: List[str] = []
    llm_available = True
    for _, row in df.iterrows():
        if not llm_available:
            summaries.append(None)
            sentiments.append(None)
            risks.append(None)
            highlights.append(None)
            continue
        try:
            result = summarize_report(row.get("body", "") or "")
            summaries.append(result.get("summary"))
            sentiments.append(result.get("sentiment"))
            risks.append(json.dumps(result.get("risks", []), ensure_ascii=False))
            highlights.append(json.dumps(result.get("highlights", []), ensure_ascii=False))
        except LLMUnavailable:
            llm_available = False
            logger.warning("LLM endpoint not configured; skipping enrichment.")
            summaries.append(None)
            sentiments.append(None)
            risks.append(None)
            highlights.append(None)
        except Exception as exc:  # noqa: BLE001
            logger.error("LLM enrichment failed: %s", exc)
            summaries.append(None)
            sentiments.append(None)
            risks.append(None)
            highlights.append(None)
    df = df.assign(summary=summaries, sentiment=sentiments, risk_tags=risks, highlights=highlights)
    return df


def load_dataframe(df: pd.DataFrame, start_date: str, end_date: str, replace: bool) -> None:
    if df.empty:
        logger.warning("No reports/news between %s and %s.", start_date, end_date)
        return
    engine = get_engine()
    with engine.begin() as connection:
        if replace:
            connection.execute(
                text("DELETE FROM news WHERE publish_time BETWEEN :start AND :end"),
                {"start": df["publish_time"].min(), "end": df["publish_time"].max()},
            )
        df.to_sql("news", connection, if_exists="append", index=False, method="multi", chunksize=500)
    logger.info("Inserted %d rows into news.", len(df))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    client = TushareClient()
    logger.info("Fetching news from %s to %s.", args.start_date, args.end_date)
    df = client.news(args.start_date, args.end_date)
    df = transform(df, args.limit)
    df = enrich(df)
    save_dataframe(df, "news", f"{args.start_date}_{args.end_date}", f"news_{args.start_date}_{args.end_date}.csv")
    load_dataframe(df, args.start_date, args.end_date, args.replace)
    logger.info("News sync completed for window %s-%s.", args.start_date, args.end_date)


if __name__ == "__main__":
    main()

