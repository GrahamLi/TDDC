#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TDCC Crawler

Builds a local database of historical shareholding distribution for Taiwan stocks.

- Reads stock codes from MoneyDJ
- Filters out non-individual stocks (ETF/ETN/bonds/etc.)
- Crawls TDCC historical weekly records for the last year per code
- Stores one folder per stock code; inside, one CSV per date (YYYY-MM-DD.csv)
- Implements retry, timeout, polite delays

Notes:
- TDCC site is dynamic. This script attempts to use requests-based form posts first.
  If that fails due to JavaScript rendering, you can re-run with --use-selenium to
  enable headless browser scraping.

CLI Example:
  python tdcc_crawler.py --output-dir data --headless --max-codes 50
"""

import argparse
import csv
import datetime as dt
import json
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    # Optional import; only needed when --use-selenium is provided
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except Exception:  # pragma: no cover - optional dependency
    webdriver = None
    ChromeOptions = None
    By = None
    WebDriverWait = None
    EC = None

MONEYDJ_STOCK_TABLE_URL = "https://moneydj.emega.com.tw/js/StockTable.htm"
TDCC_QUERY_URL = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0 Safari/537.36"
)

# Common 15-bin labels used by TDCC. These may evolve; scraper will store what is found.
DEFAULT_TDCC_BINS = [
    "1-999",
    "1,000-5,000",
    "5,001-10,000",
    "10,001-15,000",
    "15,001-20,000",
    "20,001-30,000",
    "30,001-40,000",
    "40,001-50,000",
    "50,001-100,000",
    "100,001-200,000",
    "200,001-400,000",
    "400,001-600,000",
    "600,001-800,000",
    "800,001-1,000,000",
    "1,000,001+",
]

EXCLUDE_KEYWORDS = [
    "ETF", "ETN", "債", "美債", "道瓊", "那斯達克", "NASDAQ", "S&P", "期貨",
    "原油", "黃金", "布蘭特", "紐約", "富邦臺50", "上證", "深證", "中國", "香港",
    "日經", "美元", "REIT", "指數", "基金", "反1", "反2", "正2", "正3",
]

HEADERS = {"User-Agent": DEFAULT_USER_AGENT, "Accept-Language": "zh-TW,zh;q=0.9"}


@dataclass
class StockMeta:
    code: str
    name: str


def _http_get(url: str, params: Optional[Dict[str, str]] = None, retries: int = 3, timeout: int = 20) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return resp
        except Exception:
            pass
        time.sleep(min(5 * attempt, 10))
    return None


def _http_post(url: str, data: Dict[str, str], retries: int = 3, timeout: int = 25) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, data=data, headers=HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return resp
        except Exception:
            pass
        time.sleep(min(5 * attempt, 10))
    return None


def read_moneydj_stock_codes() -> List[StockMeta]:
    resp = _http_get(MONEYDJ_STOCK_TABLE_URL)
    if resp is None:
        raise RuntimeError("Failed to fetch MoneyDJ stock table")

    # The content is JS/HTML; attempt to extract code and name pairs
    text = resp.text
    # Typical patterns like: href='...stock=2330' ... '>台積電(2330)'
    code_name_pairs: List[StockMeta] = []

    soup = BeautifulSoup(text, "html.parser")
    for a in soup.find_all("a"):
        label = a.get_text(strip=True)
        if not label:
            continue
        # Match patterns like '台積電(2330)' or '2330 台積電'
        m = re.search(r"([\u4e00-\u9fa5A-Za-z0-9\-\s]+)[(（ ]?(\d{4})[)） ]?", label)
        if not m:
            continue
        name = m.group(1).strip()
        code = m.group(2)
        code_name_pairs.append(StockMeta(code=code, name=name))

    # Deduplicate preserving order
    seen: set = set()
    unique_pairs: List[StockMeta] = []
    for pair in code_name_pairs:
        if pair.code in seen:
            continue
        seen.add(pair.code)
        unique_pairs.append(pair)

    # Filter out ETFs, etc.
    filtered: List[StockMeta] = []
    for pair in unique_pairs:
        name_upper = pair.name.upper()
        if any(kw.upper() in name_upper for kw in EXCLUDE_KEYWORDS):
            continue
        filtered.append(pair)

    if not filtered:
        # Fallback: if parsing failed, try a conservative regex in raw text
        for m in re.finditer(r"(\d{4})", text):
            code = m.group(1)
            filtered.append(StockMeta(code=code, name=f"{code}"))

    return filtered


def compute_target_dates(past_weeks: int = 52) -> List[str]:
    today = dt.date.today()
    # Use Mondays as canonical weekly anchor
    def to_monday(d: dt.date) -> dt.date:
        return d - dt.timedelta(days=d.weekday())

    dates: List[str] = []
    base = to_monday(today)
    for i in range(past_weeks, -1, -1):
        d = base - dt.timedelta(weeks=i)
        dates.append(d.isoformat())
    return dates


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def list_existing_dates(code_dir: Path) -> set:
    if not code_dir.exists():
        return set()
    return {p.stem for p in code_dir.glob("*.csv")}


def polite_sleep(min_s: float = 0.8, max_s: float = 1.8) -> None:
    time.sleep(random.uniform(min_s, max_s))


def parse_tdcc_table(html: str) -> Tuple[List[str], List[Dict[str, str]]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("TDCC table not found")

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    rows: List[Dict[str, str]] = []
    for tr in table.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds or len(tds) != len(headers):
            continue
        rows.append({h: v for h, v in zip(headers, tds)})
    return headers, rows


def fetch_tdcc_for_code_date_requests(stock_code: str, date_str: str) -> Optional[Tuple[List[str], List[Dict[str, str]]]]:
    # Attempt a POST with expected form fields. These may change; adjust as needed.
    form = {
        "stockNo": stock_code,
        "qryType": "1",  # example placeholder
        "date": date_str.replace("-", "/"),
    }
    resp = _http_post(TDCC_QUERY_URL, form)
    if resp is None:
        return None
    try:
        return parse_tdcc_table(resp.text)
    except Exception:
        return None


def fetch_tdcc_for_code_date_selenium(stock_code: str, date_str: str, headless: bool = True) -> Optional[Tuple[List[str], List[Dict[str, str]]]]:
    if webdriver is None:
        return None

    options = ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get(TDCC_QUERY_URL)
        wait = WebDriverWait(driver, 20)

        # Fill form fields: stock code and date selects.
        # NOTE: The exact element locators may need adjustments for the real page.
        code_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='stockNo']")))
        code_input.clear()
        code_input.send_keys(stock_code)

        # Select date if there is a <select>. If date is free text, fill accordingly.
        # Try to find a generic select element.
        try:
            date_select = driver.find_element(By.CSS_SELECTOR, "select[name='date']")
            for option in date_select.find_elements(By.TAG_NAME, "option"):
                if date_str in option.text or date_str.replace("-", "/") in option.text:
                    option.click()
                    break
        except Exception:
            pass

        # Submit form
        try:
            submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
            submit_btn.click()
        except Exception:
            # Fallback: press Enter
            code_input.submit()

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
        html = driver.page_source
        return parse_tdcc_table(html)
    finally:
        driver.quit()


def save_csv(code_dir: Path, date_str: str, headers: List[str], rows: List[Dict[str, str]]) -> None:
    ensure_dir(code_dir)
    csv_path = code_dir / f"{date_str}.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def crawl_code(stock: StockMeta, code_dir: Path, dates: List[str], use_selenium: bool, headless: bool) -> None:
    existing_dates = list_existing_dates(code_dir)
    for date_str in dates:
        if date_str in existing_dates:
            continue
        headers_rows: Optional[Tuple[List[str], List[Dict[str, str]]]] = fetch_tdcc_for_code_date_requests(stock.code, date_str)
        if headers_rows is None and use_selenium:
            headers_rows = fetch_tdcc_for_code_date_selenium(stock.code, date_str, headless=headless)
        if headers_rows is None:
            print(f"[WARN] Skip {stock.code} {date_str}: unable to fetch")
            continue
        headers, rows = headers_rows
        save_csv(code_dir, date_str, headers, rows)
        polite_sleep()


def main() -> None:
    parser = argparse.ArgumentParser(description="TDCC crawler - build local database of holding distributions")
    parser.add_argument("--output-dir", default="data", help="Root directory to store per-code folders")
    parser.add_argument("--max-codes", type=int, default=0, help="Limit number of codes to crawl (0 = no limit)")
    parser.add_argument("--weeks", type=int, default=52, help="How many weeks back to crawl")
    parser.add_argument("--use-selenium", action="store_true", help="Use headless browser when needed")
    parser.add_argument("--headless", action="store_true", help="Run headless browser (if using selenium)")
    parser.add_argument("--codes", nargs="*", help="Optional list of stock codes to include (override MoneyDJ)")
    args = parser.parse_args()

    output_root = Path(args.output_dir)
    ensure_dir(output_root)

    if args.codes:
        stocks = [StockMeta(code=c, name=c) for c in args.codes]
    else:
        stocks = read_moneydj_stock_codes()

    if args.max_codes > 0:
        stocks = stocks[: args.max_codes]

    dates = compute_target_dates(past_weeks=args.weeks)

    for stock in stocks:
        code_dir = output_root / stock.code
        ensure_dir(code_dir)
        try:
            crawl_code(stock, code_dir, dates, use_selenium=args.use_selenium, headless=args.headless)
        except Exception as e:
            print(f"[ERROR] {stock.code} {stock.name}: {e}")
            continue

    print("Done.")


if __name__ == "__main__":
    main()