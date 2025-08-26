"""Crawler for fetching stock holding distribution data from TDCC."""
from __future__ import annotations

import datetime as _dt
import logging
from pathlib import Path
from typing import Dict, Iterable, List

from . import fetch_stock_list
from .utils import request_with_retry, save_json

TDCC_URL = "https://www.tdcc.com.tw/smWeb/QryStockAjax.do"
logger = logging.getLogger(__name__)


def generate_past_year_dates(today: _dt.date | None = None) -> List[str]:
    """Return a list of date strings (YYYYMMDD) for the past 52 weeks."""
    if today is None:
        today = _dt.date.today()
    start = today - _dt.timedelta(days=365)
    dates = []
    current = start
    while current <= today:
        dates.append(current.strftime("%Y%m%d"))
        current += _dt.timedelta(days=7)
    return dates


def fetch_tdcc_data(stock_code: str, date: str) -> Dict:
    """Fetch TDCC holding distribution for *stock_code* at *date*.

    The returned JSON structure mirrors what the website provides. Actual keys may
    vary and callers should be prepared to handle changes.
    """
    payload = {
        "scaDates": date,
        "scaDate": date,
        "stkNo": stock_code,
    }
    logger.debug("Fetching TDCC data for %s at %s", stock_code, date)
    resp = request_with_retry("post", TDCC_URL, data=payload)
    return resp.json()


def update_stock(stock_code: str, base_dir: Path = Path("data")) -> List[str]:
    """Download new TDCC data for *stock_code*.

    Returns a list of dates that were downloaded during this invocation.
    """
    stock_dir = base_dir / stock_code
    existing = {p.stem for p in stock_dir.glob("*.json")}

    target_dates = generate_past_year_dates()
    new_dates = [d for d in target_dates if d not in existing]
    downloaded: List[str] = []
    for date in new_dates:
        try:
            data = fetch_tdcc_data(stock_code, date)
        except Exception as exc:  # pragma: no cover - network dependent
            logger.error("Failed to fetch %s %s: %s", stock_code, date, exc)
            continue
        save_json(stock_dir / f"{date}.json", data)
        downloaded.append(date)
    return downloaded


def run() -> None:
    """Entry point that fetches stock codes and updates them one by one."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    codes = fetch_stock_list.get_stock_codes()
    for code in codes:
        downloaded = update_stock(code)
        if downloaded:
            logger.info("%s: downloaded %s entries", code, len(downloaded))
        else:
            logger.info("%s: no new data", code)


if __name__ == "__main__":  # pragma: no cover - manual execution
    run()
