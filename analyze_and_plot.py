#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Analyze and Plot

- Reads Excel produced by query_and_aggregate.py
- For each metric (people, shares, percent), plots 15 trend lines (bins)
- Supports category modes that influence labeling/coloring:
  - shares: default; bins as-is by share-count bins
  - amount: requires --price; maps labels to amount ranges for legend prefix
  - custom: requires --ranges like "0-30,30-100,100-200,..." (in thousands of shares); prefixes legend
- Implements dual y-axis support and dynamic y-scale as specified
- Outputs three Excel files with one image chart each

CLI Example:
  python analyze_and_plot.py --input out/2330_summary.xlsx --category shares --out-dir out/analysis
"""

import argparse
import io
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd


def read_summary_excel(path: Path) -> Dict[str, pd.DataFrame]:
    # The writer saved three tables in a single sheet in order: people, shares, percent
    # We'll read all and infer split by empty rows
    xl = pd.ExcelFile(path)
    df = xl.parse("Summary", header=0)

    # Heuristic: locate blocks by completely empty rows
    blocks: List[Tuple[int, int]] = []
    is_empty = df.isna().all(axis=1)
    starts: List[int] = []
    for i, empty in enumerate(is_empty.values.tolist() + [True]):
        if i == 0:
            prev_empty = True
        else:
            prev_empty = is_empty.values.tolist()[i - 1] if i < len(is_empty) else False
        if prev_empty and not empty:
            starts.append(i)
        if not prev_empty and empty:
            blocks.append((starts[-1], i))

    tables: List[pd.DataFrame] = []
    for s, e in blocks:
        block = df.iloc[s:e].copy()
        tables.append(block)

    # Expect three tables
    out: Dict[str, pd.DataFrame] = {}
    keys = ["people", "shares", "percent"]
    for key, table in zip(keys, tables):
        # First column should be dates; ensure datetime index
        table = table.rename(columns={table.columns[0]: "date"})
        table["date"] = pd.to_datetime(table["date"])  # type: ignore
        table = table.set_index("date")
        out[key] = table

    return out


def apply_category_labels(columns: List[str], mode: str, price: Optional[float], custom_ranges: Optional[List[Tuple[float, float]]]) -> List[str]:
    if mode == "shares":
        return columns
    if mode == "amount":
        if not price:
            raise SystemExit("--price is required for category=amount")
        # For legend clarity, prefix with amount range estimation based on midpoints
        labeled: List[str] = []
        for label in columns:
            rng = parse_bin_label_to_shares(label)
            if rng is None:
                labeled.append(label)
                continue
            low, high = rng
            if high is None:
                s = f">= {low * price:,.0f}"
            else:
                s = f"{low * price:,.0f}-{high * price:,.0f}"
            labeled.append(f"${s} | {label}")
        return labeled
    if mode == "custom":
        if not custom_ranges:
            raise SystemExit("--ranges is required for category=custom")
        labeled: List[str] = []
        for label in columns:
            rng = parse_bin_label_to_shares(label)
            if rng is None:
                labeled.append(label)
                continue
            low, high = rng
            tag = find_custom_bucket(low, custom_ranges)
            labeled.append(f"{tag} | {label}")
        return labeled
    return columns


def parse_bin_label_to_shares(label: str) -> Optional[Tuple[float, Optional[float]]]:
    # Convert common TDCC labels to numeric ranges in shares
    s = label.replace(',', '').strip()
    if '+' in s:
        try:
            low = float(s.replace('+', ''))
            return low, None
        except Exception:
            return None
    if '-' in s:
        a, b = s.split('-', 1)
        try:
            return float(a), float(b)
        except Exception:
            return None
    # Fallback None
    return None


def parse_custom_ranges(ranges_str: str) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for part in ranges_str.split(','):
        part = part.strip()
        if not part:
            continue
        a, b = part.split('-', 1)
        out.append((float(a), float(b)))
    return out


def find_custom_bucket(value_low: float, buckets: List[Tuple[float, float]]) -> str:
    for i, (a, b) in enumerate(buckets, start=1):
        if a <= value_low <= b:
            return f"B{i}:{a}-{b}"
    return "B?"


def make_chart_image(series_df: pd.DataFrame, title: str, ylabel: str) -> bytes:
    fig, ax1 = plt.subplots(figsize=(12, 6), dpi=150)

    # Primary axis for series
    for col in series_df.columns:
        ax1.plot(series_df.index, series_df[col], linewidth=1.5, label=str(col))

    ax1.set_title(title)
    ax1.set_xlabel("Date")
    ax1.set_ylabel(ylabel)

    # Dual y-axis as a placeholder (unused data but reserved for future overlay)
    ax2 = ax1.twinx()
    ax2.set_ylabel("Secondary")
    ax2.set_yticks([])

    # Dynamic scale heuristic
    if not series_df.empty:
        y = series_df.values.flatten()
        y = y[~pd.isna(y)]
        if y.size:
            yr = float(y.max() - y.min())
            last = series_df.tail(3).values.flatten()
            last = last[~pd.isna(last)]
            if yr > 0 and last.size and (last.max() - last.min()) <= 0.01 * yr:
                ax1.set_ylim(min(y.min(), last.min()) * 0.98, max(y.max(), last.max()) * 1.02)

    ax1.legend(loc='upper left', fontsize=8, ncol=2)
    fig.autofmt_xdate()
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format='png')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def write_outputs(out_dir: Path, base_name: str, people: pd.DataFrame, shares: pd.DataFrame, percent: pd.DataFrame) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_dir / f"{base_name}_people.xlsx", engine="xlsxwriter") as w:
        people.to_excel(w, sheet_name="People")
        img = make_chart_image(people, "People (15 bins)", "People")
        img_path = out_dir / f"{base_name}_people.png"
        img_path.write_bytes(img)
        ws = w.sheets["People"]
        ws.insert_image(1, people.shape[1] + 2, str(img_path))
    with pd.ExcelWriter(out_dir / f"{base_name}_shares.xlsx", engine="xlsxwriter") as w:
        shares.to_excel(w, sheet_name="Shares")
        img = make_chart_image(shares, "Shares (15 bins)", "Shares/Units")
        img_path = out_dir / f"{base_name}_shares.png"
        img_path.write_bytes(img)
        ws = w.sheets["Shares"]
        ws.insert_image(1, shares.shape[1] + 2, str(img_path))
    with pd.ExcelWriter(out_dir / f"{base_name}_percent.xlsx", engine="xlsxwriter") as w:
        percent.to_excel(w, sheet_name="Percent")
        img = make_chart_image(percent, "Percent (15 bins)", "Percent (%)")
        img_path = out_dir / f"{base_name}_percent.png"
        img_path.write_bytes(img)
        ws = w.sheets["Percent"]
        ws.insert_image(1, percent.shape[1] + 2, str(img_path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze TDCC tables and plot 15-bin trends")
    parser.add_argument("--input", required=True, help="Excel file from query_and_aggregate.py")
    parser.add_argument("--category", choices=["shares", "amount", "custom"], default="shares")
    parser.add_argument("--price", type=float, help="Stock price for category=amount")
    parser.add_argument("--ranges", help="Custom ranges like '0-30,30-100' for category=custom (in shares)")
    parser.add_argument("--out-dir", required=True, help="Directory to write outputs")
    args = parser.parse_args()

    src = Path(args.input)
    tables = read_summary_excel(src)

    # Apply label transformations according to category mode
    ranges_parsed = parse_custom_ranges(args.ranges) if args.ranges else None

    out_tables: Dict[str, pd.DataFrame] = {}
    for key, df in tables.items():
        new_cols = apply_category_labels(df.columns.tolist(), args.category, args.price, ranges_parsed)
        new_df = df.copy()
        new_df.columns = new_cols
        out_tables[key] = new_df

    base = src.stem.replace("_summary", "")
    out_dir = Path(args.out_dir)
    write_outputs(out_dir, base, out_tables["people"], out_tables["shares"], out_tables["percent"])
    print(f"Wrote three Excel files to {out_dir}")


if __name__ == "__main__":
    main()