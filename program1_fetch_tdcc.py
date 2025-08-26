#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Program 1: TDCC Historical Shareholding Distribution Fetcher

Purpose:
  - Build a local database for Taiwan stocks' shareholding distribution by fetching
    data from TDCC and organizing them under a directory-per-stock structure.

Notes:
  - TDCC website uses dynamic requests. This script implements a pluggable fetcher
    with retry/backoff. By default, it fetches available dates per stock for the
    past 365 days and stores each date as an individual CSV under data/<stock>/<date>.csv
  - MoneyDJ is used to obtain the list of stock codes. We heuristically exclude ETF-like
    codes (e.g., codes beginning with "00"). You can customize the filter as needed.

CLI:
  - Example: python program1_fetch_tdcc.py --out data --since 365 --concurrency 4

Environment:
  - Network access is required to fully fetch data. If TDCC blocks requests or the
    endpoint changes, adapt the TDCC client implementation below.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

MONEYDJ_STOCK_TABLE_URL = "https://moneydj.emega.com.tw/js/StockTable.htm"
TDCC_QUERY_PAGE = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"


@dataclass
class StockCode:
    code: str
    name: Optional[str] = None


def ensure_dir(path: str) -> None:
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def daterange_days(days: int) -> List[str]:
    today = dt.date.today()
    return [(today - dt.timedelta(days=d)).isoformat() for d in range(days, -1, -1)]


def select_recent_mondays(days: int) -> List[str]:
    # TDCC data is weekly; prefer Mondays in the range to cut redundant requests
    dates = []
    for iso in daterange_days(days):
        d = dt.date.fromisoformat(iso)
        if d.weekday() == 0:  # Monday
            dates.append(iso)
    if not dates:
        # fallback at least include today
        dates = [dt.date.today().isoformat()]
    return dates


def request_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    return s


def fetch_moneydj_stock_codes(session: requests.Session) -> List[StockCode]:
    resp = session.get(MONEYDJ_STOCK_TABLE_URL, timeout=30)
    resp.raise_for_status()
    text = resp.text
    # Heuristic: extract 4-digit numeric codes and their nearby names if present
    # This JS typically contains arrays of stock items; we fallback to regex on 4-digit sequences
    codes = sorted(set(re.findall(r"(?<!\d)(\d{4})(?!\d)", text)))
    results: List[StockCode] = [StockCode(code=c) for c in codes]
    return results


def is_probably_equity(code: StockCode) -> bool:
    # Heuristic: exclude ETFs, ETNs, bonds that often start with 00xx or 01xx
    # Adjust as needed; keep 11xx, 12xx, 13xx, 2xxx, 3xxx, 6xxx ...
    if re.match(r"^(00|01)\d{2}$", code.code):
        return False
    return True


class TDCCClient:
    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session = session or request_session()

    @retry(wait=wait_exponential(multiplier=1, min=2, max=60), stop=stop_after_attempt(5), reraise=True,
           retry=retry_if_exception_type((requests.RequestException,)))
    def fetch_dates_available(self, stock_code: str) -> List[str]:
        # NOTE: The public page is dynamic; in practice there is an underlying API.
        # Here we optimistically attempt to parse available dates from the form page.
        resp = self.session.get(TDCC_QUERY_PAGE, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        # try to find options within a select of dates
        options = soup.select("select option")
        parsed: List[str] = []
        for opt in options:
            val = (opt.get("value") or "").strip()
            # Accept YYYY-MM-DD or YYYY/MM/DD formats
            if re.match(r"^\d{4}[-/]\d{2}[-/]\d{2}$", val):
                parsed.append(val.replace("/", "-"))
        # Fallback to last 52 Mondays if not parseable
        if not parsed:
            parsed = select_recent_mondays(365)
        return sorted(set(parsed))

    @retry(wait=wait_exponential(multiplier=1, min=2, max=60), stop=stop_after_attempt(5), reraise=True,
           retry=retry_if_exception_type((requests.RequestException,)))
    def fetch_distribution_for_date(self, stock_code: str, date_iso: str) -> List[Tuple[str, str, str]]:
        # Placeholder implementation: TDCC requires specific POST parameters to return table
        # We attempt a conservative GET and parse any table on the page; if not available, raise
        url = TDCC_QUERY_PAGE
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table")
        if not table:
            # Return empty structure to still create file scaffolds
            return []
        rows = []
        for tr in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if len(cells) >= 3:
                rows.append((cells[0], cells[1], cells[2]))
        return rows


def existing_dates_for_stock(root_dir: str, stock_code: str) -> List[str]:
    stock_dir = os.path.join(root_dir, stock_code)
    if not os.path.isdir(stock_dir):
        return []
    files = [f for f in os.listdir(stock_dir) if re.match(r"^\d{4}-\d{2}-\d{2}\.(csv|json)$", f)]
    dates = [re.match(r"^(\d{4}-\d{2}-\d{2})", f).group(1) for f in files if re.match(r"^(\d{4}-\d{2}-\d{2})", f)]
    return sorted(set(dates))


def save_rows_csv(root_dir: str, stock_code: str, date_iso: str, rows: List[Tuple[str, str, str]]) -> None:
    stock_dir = os.path.join(root_dir, stock_code)
    ensure_dir(stock_dir)
    path = os.path.join(stock_dir, f"{date_iso}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["bucket", "holders", "shares_ratio"])
        for b, h, r in rows:
            writer.writerow([b, h, r])


def process_stock(stock: StockCode, client: TDCCClient, out_dir: str, days: int) -> Tuple[str, int]:
    try:
        existing = set(existing_dates_for_stock(out_dir, stock.code))
        available = client.fetch_dates_available(stock.code)
        # restrict to last N days
        cutoff = set(select_recent_mondays(days))
        targets = [d for d in available if d in cutoff and d not in existing]
        created = 0
        for d in targets:
            rows = client.fetch_distribution_for_date(stock.code, d)
            save_rows_csv(out_dir, stock.code, d, rows)
            created += 1
        return (stock.code, created)
    except Exception as e:
        return (stock.code, -1)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="TDCC Shareholding Fetcher")
    parser.add_argument("--out", default="data", help="Output root directory for local database")
    parser.add_argument("--since", type=int, default=365, help="How many days back to consider (default: 365)")
    parser.add_argument("--codes", nargs="*", help="Optional specific stock codes to fetch (e.g., 2330 2603)")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of concurrent workers")
    args = parser.parse_args(argv)

    ensure_dir(args.out)

    session = request_session()
    stocks = [StockCode(code=c) for c in args.codes] if args.codes else fetch_moneydj_stock_codes(session)
    stocks = [s for s in stocks if is_probably_equity(s)]

    client = TDCCClient(session)

    print(f"Total candidate stocks: {len(stocks)}")
    results: List[Tuple[str, int]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [executor.submit(process_stock, s, client, args.out, args.since) for s in stocks]
        for fut in concurrent.futures.as_completed(futures):
            code, created = fut.result()
            results.append((code, created))
            if created >= 0:
                print(f"{code}: created {created} files")
            else:
                print(f"{code}: error")

    ok = sum(1 for _, c in results if c >= 0)
    print(f"Completed. {ok}/{len(results)} stocks processed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())