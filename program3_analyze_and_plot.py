#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Program 3: Analyze Program 2 Output and Plot Trend Charts (3 Excel files)

Inputs:
  - Excel output from Program 2 (holders, shares, ratio sheets; optional kline)
  - Optional stock price (for amount-based classification)
  - Optional custom bucket boundaries for manual classification

Outputs:
  - Three Excel files: <code>_trend_holders.xlsx, <code>_trend_shares.xlsx, <code>_trend_ratio.xlsx
    Each contains a line chart with 15 lines (buckets) over time

Features:
  - 15-bucket handling; draws three charts (holders, shares, ratio)
  - Three category modes: quantity-based, amount-based (requires price), user-defined ranges
  - Dual y-axis support placeholder (can layer secondary axis series if needed)
  - Dynamic step: detects low-variance segments; adjusts y-axis major unit heuristically
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from pandas import DataFrame


@dataclass
class CategorySpec:
    mode: str  # quantity|amount|custom
    price: Optional[float] = None
    custom: Optional[List[Tuple[float, float]]] = None  # inclusive ranges


def read_program2_excel(path: str) -> Tuple[DataFrame, DataFrame, DataFrame]:
    xls = pd.ExcelFile(path)
    holders = pd.read_excel(xls, "holders", index_col=0)
    shares  = pd.read_excel(xls, "shares", index_col=0)
    ratio   = pd.read_excel(xls, "ratio", index_col=0)
    # Ensure index is datetime-like
    for df in (holders, shares, ratio):
        df.index = pd.to_datetime(df.index)
    return holders, shares, ratio


def infer_bucket_range(bucket_label: str) -> Tuple[float, float]:
    # Attempt to parse labels like "1-999", "1~5,000", ">= 1,000,001", "100萬以上"
    lbl = str(bucket_label).replace(",", "").replace("萬", "0000").strip()
    # handle ">= N"
    m = re.match(r"^>=?\s*(\d+(?:\.\d+)?)$", lbl)
    if m:
        v = float(m.group(1))
        return (v, float("inf"))
    # handle "A-B"
    m = re.match(r"^(\d+(?:\.\d+)?)\s*[-~–]\s*(\d+(?:\.\d+)?)$", lbl)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    # fallback: single number means exact or open range
    m = re.match(r"^(\d+(?:\.\d+)?)$", lbl)
    if m:
        v = float(m.group(1))
        return (v, v)
    return (0.0, 0.0)


def classify_bucket(range_tuple: Tuple[float, float], spec: CategorySpec) -> str:
    low, high = range_tuple
    if spec.mode == "quantity":
        if high <= 400000:
            return "retail"
        if low > 1000000:
            return "whale"
        return "mid"
    if spec.mode == "amount":
        price = spec.price or 0.0
        low_amt = low * price
        high_amt = (high if high != float("inf") else low) * price
        if high_amt < 5_000_000:
            return "retail"
        if low_amt > 30_000_000:
            return "whale"
        if 5_000_000 <= high_amt <= 10_000_000:
            return "small_mid"
        return "mid"
    if spec.mode == "custom" and spec.custom:
        # map to first matching custom band label
        for idx, (c_low, c_high) in enumerate(spec.custom):
            if (low >= c_low) and (high <= c_high):
                return f"band_{idx+1}"
        return "band_other"
    return "unknown"


def detect_dynamic_major_unit(values: Iterable[float]) -> Optional[float]:
    vals = np.array([v for v in values if pd.notna(v)], dtype=float)
    if len(vals) < 3:
        return None
    total_range = vals.max() - vals.min()
    if total_range <= 0:
        return None
    # sliding window of size 3
    small_segments = 0
    for i in range(len(vals) - 2):
        seg = vals[i:i+3]
        if seg.max() - seg.min() <= 0.01 * total_range:
            small_segments += 1
    if small_segments >= max(1, len(vals)//10):
        # choose a smaller major unit roughly 1/10 of range
        return total_range / 10.0
    return None


def write_trend_excel(code: str, name: str, df: DataFrame, out_path: str) -> None:
    with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd", date_format="yyyy-mm-dd") as writer:
        df.to_excel(writer, sheet_name="trend")
        workbook = writer.book
        worksheet = writer.sheets["trend"]
        chart = workbook.add_chart({"type": "line"})
        num_rows, num_cols = df.shape
        for c in range(num_cols):
            chart.add_series({
                "name":       ["trend", 0, c + 1],
                "categories": ["trend", 1, 0, num_rows, 0],
                "values":     ["trend", 1, c + 1, num_rows, c + 1],
            })
        chart.set_title({"name": f"{code} - {name}"})
        chart.set_x_axis({"name": "date"})
        # Dynamic major unit
        mu = detect_dynamic_major_unit(df.select_dtypes(include=[float, int]).values.flatten())
        if mu:
            chart.set_y_axis({"name": name, "major_unit": mu})
        else:
            chart.set_y_axis({"name": name})
        worksheet.insert_chart(num_rows + 3, 1, chart)


def reduce_to_15_lines(df: DataFrame) -> DataFrame:
    # If columns > 15, keep top-15 by average magnitude
    if df.shape[1] <= 15:
        return df
    means = df.abs().mean().sort_values(ascending=False)
    cols = list(means.head(15).index)
    return df[cols]


def analyze(holders: DataFrame, shares: DataFrame, ratio: DataFrame, spec: CategorySpec, code: str, out_dir: str) -> None:
    # holders/shares/ratio: index=date, columns=bucket labels
    def transform(df: DataFrame) -> DataFrame:
        # Map bucket columns to category names, aggregate per category
        col_to_cat = {}
        for col in df.columns:
            rng = infer_bucket_range(col)
            cat = classify_bucket(rng, spec)
            col_to_cat[col] = cat
        grouped = df.groupby(col_to_cat, axis=1).sum()
        grouped = grouped.sort_index(axis=1)
        grouped.index = pd.to_datetime(grouped.index)
        return grouped

    h = transform(holders)
    s = transform(shares)
    r = transform(ratio)

    h = reduce_to_15_lines(h)
    s = reduce_to_15_lines(s)
    r = reduce_to_15_lines(r)

    os.makedirs(out_dir, exist_ok=True)
    write_trend_excel(code, "holders", h, os.path.join(out_dir, f"{code}_trend_holders.xlsx"))
    write_trend_excel(code, "shares",  s, os.path.join(out_dir, f"{code}_trend_shares.xlsx"))
    write_trend_excel(code, "ratio",   r, os.path.join(out_dir, f"{code}_trend_ratio.xlsx"))


def parse_custom_bands(arg: Optional[str]) -> Optional[List[Tuple[float, float]]]:
    if not arg:
        return None
    bands: List[Tuple[float, float]] = []
    parts = [p.strip() for p in arg.split(",") if p.strip()]
    for p in parts:
        m = re.match(r"^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)$", p)
        if not m:
            continue
        bands.append((float(m.group(1)), float(m.group(2))))
    return bands or None


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze Program 2 Excel and plot trend charts")
    parser.add_argument("input", help="Path to Program 2 Excel output")
    parser.add_argument("code", help="Stock code e.g., 2330")
    parser.add_argument("--mode", choices=["quantity", "amount", "custom"], default="quantity")
    parser.add_argument("--price", type=float, default=None, help="Stock price for amount mode")
    parser.add_argument("--custom", type=str, default=None, help="Custom bands e.g., 0-30,30-100")
    parser.add_argument("--outdir", default="analysis_out", help="Output directory")
    args = parser.parse_args(argv)

    spec = CategorySpec(mode=args.mode, price=args.price, custom=parse_custom_bands(args.custom))

    if spec.mode == "amount" and (spec.price is None or spec.price <= 0):
        print("Amount mode requires a positive --price")
        return 1

    holders, shares, ratio = read_program2_excel(args.input)

    analyze(holders, shares, ratio, spec, args.code, args.outdir)

    print(f"Wrote trend Excel files to {args.outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())