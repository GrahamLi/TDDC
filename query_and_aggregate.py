#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Query and Aggregate

- Reads local TDCC database built by tdcc_crawler.py
- User provides stock code and date range
- Selects closest weekly records per boundary with fallback and warns
- Builds three pivot tables (index=date, columns=TDCC bins):
  1) people_count
  2) shares_count
  3) percent_of_inventory
- Fetches weekly K-line and volume data from Wearn.com
- Generates three matplotlib charts overlaying K-line/volume behind our indicators
- Outputs a single Excel file containing the three tables and embedded chart images

CLI Example:
  python query_and_aggregate.py --db-root data --code 2330 --start 2024-01-01 --end 2024-08-26 --out out/2330_summary.xlsx
"""

import argparse
import datetime as dt
import io
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "zh-TW,zh;q=0.9"}


def list_available_dates(code_dir: Path) -> List[dt.date]:
    dates: List[dt.date] = []
    for p in sorted(code_dir.glob("*.csv")):
        try:
            dates.append(dt.date.fromisoformat(p.stem))
        except Exception:
            continue
    return sorted(dates)


def pick_boundary_date(available: List[dt.date], target: dt.date, prefer_ge: bool) -> Tuple[Optional[dt.date], bool]:
    if not available:
        return None, False
    if prefer_ge:
        ge = [d for d in available if d >= target]
        if ge:
            return ge[0], False
        # fallback to closest <=
        le = [d for d in reversed(available) if d <= target]
        if le:
            return le[0], True
        return available[0], True
    else:
        le = [d for d in available if d <= target]
        if le:
            return le[-1], False
        ge = [d for d in available if d >= target]
        if ge:
            return ge[0], True
        return available[-1], True


def load_tdcc_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # Try to normalize column names for three metrics
    cols = [c.strip() for c in df.columns]
    df.columns = cols

    # Heuristics for TDCC bins and metrics
    # Expected columns may include: 等級/級距, 人數, 股數, 占集保庫存數比例(%)
    # Normalize to: bin_label, people, shares, percent
    mapping: Dict[str, str] = {}
    for c in df.columns:
        if re_match(c, ["等級", "級距", "持股分級", "區間", "區間別", "持股區間", "股數級距", "張數級距", "持股張數級距"]):
            mapping[c] = "bin_label"
        elif re_match(c, ["人數"]):
            mapping[c] = "people"
        elif re_match(c, ["股數", "單位數"]):
            mapping[c] = "shares"
        elif re_match(c, ["占集保庫存數比例", "比例", "%", "佔比", "占比"]):
            mapping[c] = "percent"

    df = df.rename(columns=mapping)
    required = {"bin_label", "people", "shares", "percent"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {missing} in {csv_path}")

    return df[["bin_label", "people", "shares", "percent"]]


def re_match(text: str, keywords: List[str]) -> bool:
    t = text.replace(" ", "").replace("_", "").lower()
    for k in keywords:
        k2 = k.replace(" ", "").replace("_", "").lower()
        if k2 in t:
            return True
    return False


def build_pivots(code_dir: Path, date_range: List[dt.date]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows_people: List[pd.Series] = []
    rows_shares: List[pd.Series] = []
    rows_percent: List[pd.Series] = []

    for d in date_range:
        csv_path = code_dir / f"{d.isoformat()}.csv"
        if not csv_path.exists():
            continue
        df = load_tdcc_csv(csv_path)
        s_people = df.set_index("bin_label")["people"].astype(float)
        s_shares = df.set_index("bin_label")["shares"].astype(float)
        s_percent = df.set_index("bin_label")["percent"].astype(float)
        s_people.name = d
        s_shares.name = d
        s_percent.name = d
        rows_people.append(s_people)
        rows_shares.append(s_shares)
        rows_percent.append(s_percent)

    people_df = pd.DataFrame(rows_people).sort_index()
    shares_df = pd.DataFrame(rows_shares).sort_index()
    percent_df = pd.DataFrame(rows_percent).sort_index()

    return people_df, shares_df, percent_df


def fetch_wearn_weekly(code: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    # Wearn uses ROC year; we might need multiple months. We'll fetch a range of months and combine.
    # For simplicity, pull 15 months around the range.
    frames: List[pd.DataFrame] = []
    cur = start.replace(day=1)
    end_month = end.replace(day=1)
    months: List[Tuple[int, int]] = []
    while cur <= end_month:
        months.append((cur.year, cur.month))
        # advance by 1 month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    for y, m in months:
        roc_year = y - 1911
        url = f"https://stock.wearn.com/cdata.asp?Year={roc_year:03d}&month={m:02d}&kind={code}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            # The response is often HTML/JS with comma-separated values lines
            lines = [ln.strip() for ln in resp.text.splitlines() if ln.strip()]
            # Try to locate data lines with date and OHLCV
            data_rows: List[Tuple[dt.date, float, float]] = []  # date, close, volume
            for ln in lines:
                # Sample CSV-like: 2024/04/05,開,高,低,收,量
                parts = [p.strip() for p in ln.split(',')]
                if len(parts) < 6:
                    continue
                if not re_date(parts[0]):
                    continue
                date = parse_date(parts[0])
                try:
                    close = float(clean_num(parts[4]))
                    volume = float(clean_num(parts[5]))
                except Exception:
                    continue
                data_rows.append((date, close, volume))
            if data_rows:
                dfm = pd.DataFrame(data_rows, columns=["date", "close", "volume"]).set_index("date")
                frames.append(dfm)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["close", "volume"])

    df = pd.concat(frames).sort_index()
    df = df.loc[(df.index >= start) & (df.index <= end)]
    return df


def re_date(s: str) -> bool:
    s2 = s.replace('-', '/').strip()
    try:
        dt.datetime.strptime(s2, "%Y/%m/%d")
        return True
    except Exception:
        return False


def parse_date(s: str) -> dt.date:
    s2 = s.replace('-', '/').strip()
    return dt.datetime.strptime(s2, "%Y/%m/%d").date()


def clean_num(s: str) -> str:
    return s.replace(',', '').replace('%', '').strip()


def plot_with_kline(background: pd.DataFrame, series_df: pd.DataFrame, title: str, ylabel: str) -> bytes:
    # background: index=date; columns close, volume
    fig, ax1 = plt.subplots(figsize=(12, 6), dpi=150)

    # Plot K-line close as grey background line
    if not background.empty:
        ax1.plot(background.index, background["close"], color="#bbbbbb", linewidth=1.2, label="Close (Wearn)")

    # Second axis for volume
    ax2 = ax1.twinx()
    if not background.empty:
        ax2.bar(background.index, background["volume"], color="#e0e0ff", width=3, alpha=0.3, label="Volume")

    # Overlay our series (multiple columns)
    for col in series_df.columns:
        ax1.plot(series_df.index, series_df[col], linewidth=1.6, label=str(col))

    ax1.set_title(title)
    ax1.set_xlabel("Date")
    ax1.set_ylabel(ylabel)
    ax2.set_ylabel("Volume")

    # Dynamic y-scale: if last 3-point window variation <1% of range, tighten limits
    if not series_df.empty:
        y = series_df.values.flatten()
        y = y[~pd.isna(y)]
        if y.size > 0:
            yr = float(y.max() - y.min())
            if yr > 0:
                last = series_df.tail(3).values.flatten()
                last = last[~pd.isna(last)]
                if last.size > 0 and (last.max() - last.min()) <= 0.01 * yr:
                    ax1.set_ylim(min(y.min(), last.min()) * 0.98, max(y.max(), last.max()) * 1.02)

    # Legends
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2, loc='upper left', fontsize=8, ncol=2)

    fig.autofmt_xdate()
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format='png')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def write_excel(out_path: Path, people: pd.DataFrame, shares: pd.DataFrame, percent: pd.DataFrame, kline: pd.DataFrame) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        sheet = "Summary"
        start_row = 0
        people.to_excel(writer, sheet_name=sheet, startrow=start_row, startcol=0)
        start_row += len(people) + 3
        shares.to_excel(writer, sheet_name=sheet, startrow=start_row, startcol=0)
        start_row += len(shares) + 3
        percent.to_excel(writer, sheet_name=sheet, startrow=start_row, startcol=0)

        workbook = writer.book
        worksheet = writer.sheets[sheet]

        # Create charts as images and insert
        img_people = plot_with_kline(kline, people, title="People vs K-line", ylabel="People")
        img_shares = plot_with_kline(kline, shares, title="Shares vs K-line", ylabel="Shares/Units")
        img_percent = plot_with_kline(kline, percent, title="Percent vs K-line", ylabel="Percent (%)")

        img_dir = out_path.parent / "._charts"
        img_dir.mkdir(parents=True, exist_ok=True)
        img1_path = img_dir / f"{out_path.stem}_people.png"
        img2_path = img_dir / f"{out_path.stem}_shares.png"
        img3_path = img_dir / f"{out_path.stem}_percent.png"
        img1_path.write_bytes(img_people)
        img2_path.write_bytes(img_shares)
        img3_path.write_bytes(img_percent)

        # Insert images beneath each table
        r0 = 1
        worksheet.insert_image(r0, 8, str(img1_path))
        r1 = len(people) + 5
        worksheet.insert_image(r1, 8, str(img2_path))
        r2 = r1 + len(shares) + 5
        worksheet.insert_image(r2, 8, str(img3_path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Query local TDCC data and build Excel with tables and charts")
    parser.add_argument("--db-root", default="data", help="Root path of local database")
    parser.add_argument("--code", required=True, help="Stock code, e.g., 2330")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--out", required=True, help="Output Excel path")
    args = parser.parse_args()

    db_root = Path(args.db_root)
    code_dir = db_root / args.code
    if not code_dir.exists():
        raise SystemExit(f"Code directory not found: {code_dir}")

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)

    dates = list_available_dates(code_dir)
    if not dates:
        raise SystemExit("No CSV files found for this code")

    start_use, warn_start = pick_boundary_date(dates, start, prefer_ge=True)
    end_use, warn_end = pick_boundary_date(dates, end, prefer_ge=False)
    if start_use is None or end_use is None:
        raise SystemExit("Unable to locate boundary dates")

    if warn_start:
        print(f"[WARN] Start boundary adjusted to {start_use}")
    if warn_end:
        print(f"[WARN] End boundary adjusted to {end_use}")

    date_range = [d for d in dates if start_use <= d <= end_use]

    people, shares, percent = build_pivots(code_dir, date_range)

    kline = fetch_wearn_weekly(args.code, start_use, end_use)

    out_path = Path(args.out)
    write_excel(out_path, people, shares, percent, kline)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()