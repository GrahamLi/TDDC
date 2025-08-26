"""Utilities to fetch and filter Taiwanese stock codes from MoneyDJ."""
from __future__ import annotations

import logging
import re
from typing import Iterable, List, Tuple

import requests
from bs4 import BeautifulSoup

# URL hosting the table with all Taiwanese stock codes
MONEYDJ_STOCK_TABLE = "https://moneydj.emega.com.tw/js/StockTable.htm"

# Patterns that indicate an entry should be excluded. These cover ETFs, bonds, warrants
# and other non-equity instruments. The list can be extended over time.
EXCLUDE_KEYWORDS = ["ETF", "債", "受益", "購", "權證"]

logger = logging.getLogger(__name__)


def fetch_stock_table(url: str = MONEYDJ_STOCK_TABLE) -> str:
    """Return the raw HTML/JS content hosting the stock table.

    A small wrapper is used so that request related issues can be logged and
    retried by callers if desired.
    """
    logger.debug("Fetching stock table from %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_stock_codes(html: str) -> List[Tuple[str, str]]:
    """Parse the MoneyDJ table and return a list of (code, name) tuples.

    The file mixes HTML and JavaScript; the approach here is to let BeautifulSoup
    extract table rows and then use regular expressions for additional safety.
    """
    soup = BeautifulSoup(html, "lxml")
    codes: List[Tuple[str, str]] = []
    for row in soup.find_all("tr"):
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) >= 2 and re.fullmatch(r"\d{4}", cols[0]):
            codes.append((cols[0], cols[1]))
    return codes


def filter_stock_codes(codes: Iterable[Tuple[str, str]]) -> List[str]:
    """Filter out ETFs, bonds and other non-stock entries.

    The filter is keyword based and keeps only the numeric stock codes.
    """
    filtered: List[str] = []
    for code, name in codes:
        if any(kw in name for kw in EXCLUDE_KEYWORDS):
            logger.debug("Excluding %s %s", code, name)
            continue
        filtered.append(code)
    return filtered


def get_stock_codes() -> List[str]:
    """Convenience function combining fetch, parse and filter steps."""
    html = fetch_stock_table()
    codes = parse_stock_codes(html)
    return filter_stock_codes(codes)

__all__ = [
    "get_stock_codes",
    "fetch_stock_table",
    "parse_stock_codes",
    "filter_stock_codes",
]
