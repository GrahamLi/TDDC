#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Program 2: Query and Organize Data with K-line Overlay

Inputs:
  - Stock code, start date, end date (ISO YYYY-MM-DD)
  - Local database from Program 1: data/<stock>/<date>.csv
  - K-line source (weekly): https://stock.wearn.com/cdata.asp?Year=112&month=04&kind=8069

Outputs:
  - An Excel file with three sheets or one sheet containing three tables:
    1) holders count by bucket (TDCC buckets)
    2) shares/units by bucket
    3) holding ratio (%) by bucket
  - Each table has an overlaid chart with the weekly K-line and volume as background series

Notes:
  - Implements nearest-week selection with forward preference, falling back backward with warning.
  - Web formats may change; adjust the K-line parser if needed.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from pandas import DataFrame

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

WEARN_WEEKLY_URL_TMPL = "https://stock.wearn.com/cdata.asp?Year={roc_year}&month={month:02d}&kind={code}"


@dataclass
class Selection:
    dates: List[str]
    forward_fallback_used: bool
    backward_fallback_used: bool


def list_available_dates(root_dir: str, code: str) -> List[str]:
    stock_dir = os.path.join(root_dir, code)
    if not os.path.isdir(stock_dir):
        return []
    files = [f for f in os.listdir(stock_dir) if re.match(r"^\d{4}-\d{2}-\d{2}\.csv$", f)]
    dates = [f.split(".")[0] for f in files]
    return sorted(set(dates))


def select_dates_between(available: List[str], start: str, end: str) -> Selection:
    # Prefer >= start and <= end; if none on boundaries, fallback to closest earlier
    a = [dt.date.fromisoformat(d) for d in available]
    s = dt.date.fromisoformat(start)
    e = dt.date.fromisoformat(end)

    chosen: List[dt.date] = [d for d in a if s <= d <= e]

    forward = False
    backward = False

    if not chosen:
        # find closest after start
        after = [d for d in a if d >= s]
        if after:
            first = min(after)
            # ensure end boundary
            chosen = [d for d in a if first <= d <= e]
            forward = True
        if not chosen:
            # fallback to before start
            before = [d for d in a if d <= s]
            if before:
                last = max(before)
                chosen = [d for d in a if last <= d <= e]
                backward = True

    return Selection(dates=[d.isoformat() for d in chosen], forward_fallback_used=forward, backward_fallback_used=backward)


def read_local_frames(root_dir: str, code: str, dates: List[str]) -> Dict[str, DataFrame]:
    frames: Dict[str, DataFrame] = {}
    for d in dates:
        path = os.path.join(root_dir, code, f"{d}.csv")
        if not os.path.isfile(path):
            continue
        df = pd.read_csv(path)
        frames[d] = df
    return frames


def roc_year(year: int) -> int:
    return year - 1911


def fetch_wearn_weekly(code: str, start: dt.date, end: dt.date) -> DataFrame:
    # Wearn splits by month; fetch months in range and concatenate
    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    def month_iter(s: dt.date, e: dt.date) -> List[Tuple[int, int]]:
        res = []
        cur = dt.date(s.year, s.month, 1)
        while cur <= e:
            res.append((cur.year, cur.month))
            # next month
            if cur.month == 12:
                cur = dt.date(cur.year + 1, 1, 1)
            else:
                cur = dt.date(cur.year, cur.month + 1, 1)
        return res

    frames: List[DataFrame] = []
    for y, m in month_iter(start, end):
        url = WEARN_WEEKLY_URL_TMPL.format(roc_year=roc_year(y), month=m, code=code)
        r = session.get(url, timeout=30)
        r.raise_for_status()
        txt = r.text
        # crude parse: lines with comma-separated weekly data; attempt to detect typical format
        lines = [ln.strip() for ln in txt.splitlines() if "," in ln]
        buf = io.StringIO("\n".join(lines))
        try:
            df = pd.read_csv(buf, header=None)
            frames.append(df)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df_all = pd.concat(frames, ignore_index=True)
    # Try to coerce columns: assume 6-7 columns and the first is date-like
    # Keep only essential
    df_all = df_all.rename(columns={0: "date", 1: "open", 2: "high", 3: "low", 4: "close", 5: "volume"})
    # Clean date: accept ROC or YYYY/MM/DD
    def parse_date(v: str) -> Optional[dt.date]:
        v = str(v)
        v = v.replace(" ", "").replace("/", "-")
        m = re.match(r"^(\d{3})-(\d{2})-(\d{2})$", v)
        if m:
            y = int(m.group(1)) + 1911
            return dt.date(y, int(m.group(2)), int(m.group(3)))
        m2 = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", v)
        if m2:
            return dt.date(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
        return None

    df_all["date"] = df_all["date"].map(parse_date)
    df_all = df_all.dropna(subset=["date"]).copy()
    df_all = df_all[(df_all["date"] >= start) & (df_all["date"] <= end)].copy()
    return df_all[["date", "open", "high", "low", "close", "volume"]]


def build_tables(frames_by_date: Dict[str, DataFrame]) -> Tuple[DataFrame, DataFrame, DataFrame]:
    # Concatenate by date with bucket columns bucket, holders, shares_ratio
    rows_holders = []
    rows_shares = []
    rows_ratio = []
    for d, df in sorted(frames_by_date.items()):
        if not {"bucket", "holders", "shares_ratio"}.issubset(set(df.columns)):
            continue
        df2 = df.copy()
        df2["date"] = d
        for _, r in df2.iterrows():
            rows_holders.append({"date": d, "bucket": r["bucket"], "value": pd.to_numeric(r["holders"], errors="coerce")})
            rows_shares.append({"date": d, "bucket": r["bucket"], "value": pd.to_numeric(r.get("shares", r["holders"]), errors="coerce")})
            rows_ratio.append({"date": d, "bucket": r["bucket"], "value": pd.to_numeric(r["shares_ratio"], errors="coerce")})

    t1 = pd.DataFrame(rows_holders)
    t2 = pd.DataFrame(rows_shares)
    t3 = pd.DataFrame(rows_ratio)
    return (t1, t2, t3)


def pivot_for_chart(df: DataFrame) -> DataFrame:
    if df.empty:
        return df
    p = df.pivot_table(index="date", columns="bucket", values="value", aggfunc="sum")
    p = p.sort_index()
    return p


def write_excel_with_charts(code: str, t1: DataFrame, t2: DataFrame, t3: DataFrame, kline: DataFrame, out_path: str, warnings: List[str]) -> None:
    with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd", date_format="yyyy-mm-dd") as writer:
        # Write tables
        t1p = pivot_for_chart(t1)
        t2p = pivot_for_chart(t2)
        t3p = pivot_for_chart(t3)
        t1p.to_excel(writer, sheet_name="holders")
        t2p.to_excel(writer, sheet_name="shares")
        t3p.to_excel(writer, sheet_name="ratio")
        # Write kline
        kline_sheet = "kline"
        kline.to_excel(writer, sheet_name=kline_sheet, index=False)
        # Write warnings
        if warnings:
            pd.DataFrame({"warnings": warnings}).to_excel(writer, sheet_name="warnings", index=False)

        workbook  = writer.book
        # Create charts for each table
        def add_chart(sheet_name: str, table_df: DataFrame, start_row: int = 1, start_col: int = 1) -> None:
            if table_df.empty:
                return
            chart = workbook.add_chart({"type": "line"})
            # Add series per bucket
            num_rows, num_cols = table_df.shape
            for c in range(num_cols):
                chart.add_series({
                    "name":       [sheet_name, 0, c + 1],
                    "categories": [sheet_name, 1, 0, num_rows, 0],
                    "values":     [sheet_name, 1, c + 1, num_rows, c + 1],
                })
            chart.set_title({"name": f"{code} - {sheet_name}"})
            chart.set_x_axis({"name": "date"})
            chart.set_y_axis({"name": sheet_name})
            # Insert chart
            writer.sheets[sheet_name].insert_chart(start_row + num_rows + 3, start_col, chart)

        add_chart("holders", t1p)
        add_chart("shares", t2p)
        add_chart("ratio", t3p)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Query and organize local TDCC data with K-line overlay")
    parser.add_argument("code", help="Stock code e.g., 2330")
    parser.add_argument("start", help="Start date YYYY-MM-DD")
    parser.add_argument("end", help="End date YYYY-MM-DD")
    parser.add_argument("--root", default="data", help="Local database root directory")
    parser.add_argument("--out", default=None, help="Output Excel path (default: out/<code>_<start>_<end>.xlsx)")
    args = parser.parse_args(argv)

    available = list_available_dates(args.root, args.code)
    if not available:
        print("No local data available. Run program1 first.")
        return 1

    sel = select_dates_between(available, args.start, args.end)
    warnings: List[str] = []
    if sel.forward_fallback_used:
        warnings.append("Start date not found. Used closest later week.")
    if sel.backward_fallback_used:
        warnings.append("No later week found. Used closest earlier week.")

    frames = read_local_frames(args.root, args.code, sel.dates)

    if not frames:
        print("No frames found in selected range.")
        return 1

    t1, t2, t3 = build_tables(frames)

    s_date = dt.date.fromisoformat(sel.dates[0])
    e_date = dt.date.fromisoformat(sel.dates[-1])

    kline = fetch_wearn_weekly(args.code, s_date, e_date)

    out_dir = os.path.join("out")
    os.makedirs(out_dir, exist_ok=True)
    out_path = args.out or os.path.join(out_dir, f"{args.code}_{args.start}_{args.end}.xlsx")

    write_excel_with_charts(args.code, t1, t2, t3, kline, out_path, warnings)

    print(f"Wrote {out_path}")
    if warnings:
        for w in warnings:
            print("Warning:", w)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())