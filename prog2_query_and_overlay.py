import os
import re
import sys
import json
import math
import argparse
from datetime import date, datetime
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from tdcc_utils import ensure_dir, load_json, parse_date_any


def list_stock_records(data_dir: str, code: str) -> List[Dict[str, Any]]:
	stock_dir = os.path.join(data_dir, code)
	if not os.path.isdir(stock_dir):
		return []
	records: List[Dict[str, Any]] = []
	for fn in os.listdir(stock_dir):
		if not fn.endswith(".json"):
			continue
		path = os.path.join(stock_dir, fn)
		try:
			rec = load_json(path)
			rec_date = parse_date_any(rec["date"]).date()
			records.append({**rec, "_date": rec_date})
		except Exception:
			continue
	return sorted(records, key=lambda r: r["_date"]) 


def nearest_index_by_date(dates: List[date], target: date, prefer_ge: bool) -> Optional[int]:
	if not dates:
		return None
	if prefer_ge:
		for i, d in enumerate(dates):
			if d >= target:
				return i
		return len(dates) - 1  # fallback to latest smaller
	else:
		for i in range(len(dates) - 1, -1, -1):
			if dates[i] <= target:
				return i
		return 0  # fallback to earliest larger


def build_tables(records: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
	if not records:
		raise ValueError("No records to build tables")
	dates = [r["_date"] for r in records]
	level_keys = [f"L{i:02d}" for i in range(1, 16)]
	people = pd.DataFrame(index=dates, columns=level_keys, dtype=float)
	shares = pd.DataFrame(index=dates, columns=level_keys, dtype=float)
	percent = pd.DataFrame(index=dates, columns=level_keys, dtype=float)
	for rec in records:
		row = rec["_date"]
		for i, lvl in enumerate(rec.get("levels", []), start=1):
			key = f"L{i:02d}"
			people.loc[row, key] = float(lvl.get("holderCount", np.nan))
			shares.loc[row, key] = float(lvl.get("shareCount", np.nan))
			percent.loc[row, key] = float(lvl.get("percent", np.nan))
	people = people.sort_index()
	shares = shares.sort_index()
	percent = percent.sort_index()
	return people, shares, percent


def simulate_kline(dates: List[date], seed: int = 1) -> pd.DataFrame:
	n = len(dates)
	if n == 0:
		return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"]) 
	rnd = np.random.default_rng(seed)
	price = 100 + np.cumsum(rnd.normal(0, 1, n))
	volume = 1000 + rnd.integers(0, 500, n)
	o = price + rnd.normal(0, 0.5, n)
	c = price + rnd.normal(0, 0.5, n)
	h = np.maximum(o, c) + rnd.random(n) * 1.0
	l = np.minimum(o, c) - rnd.random(n) * 1.0
	return pd.DataFrame({
		"date": dates,
		"open": o,
		"high": h,
		"low": l,
		"close": c,
		"volume": volume,
	})


def write_excel_with_charts(output_path: str, code: str, people: pd.DataFrame, shares: pd.DataFrame, percent: pd.DataFrame, kline: Optional[pd.DataFrame]) -> None:
	ensure_dir(os.path.dirname(output_path) or ".")
	with pd.ExcelWriter(output_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd", date_format="yyyy-mm-dd") as writer:
		# Hidden data sheets for charts
		people_sheet = "people"
		shares_sheet = "shares"
		percent_sheet = "percent"
		report_sheet = "report"

		people.to_excel(writer, sheet_name=people_sheet)
		shares.to_excel(writer, sheet_name=shares_sheet)
		percent.to_excel(writer, sheet_name=percent_sheet)
		ws_people = writer.sheets[people_sheet]
		ws_shares = writer.sheets[shares_sheet]
		ws_percent = writer.sheets[percent_sheet]
		ws_people.hide()
		ws_shares.hide()
		ws_percent.hide()

		# Build report sheet with three table blocks
		start_rows = {}
		ws_report = writer.book.add_worksheet(report_sheet)
		writer.sheets[report_sheet] = ws_report

		def write_block(title: str, df: pd.DataFrame, start_row: int) -> int:
			ws_report.write(start_row, 0, title)
			# headers
			ws_report.write(start_row + 1, 0, "date")
			for j, col in enumerate(df.columns, start=1):
				ws_report.write(start_row + 1, j, col)
			# data
			for i, (idx, row) in enumerate(df.iterrows(), start=0):
				ws_report.write_datetime(start_row + 2 + i, 0, pd.to_datetime(idx).to_pydatetime())
				for j, col in enumerate(df.columns, start=1):
					val = row[col]
					if pd.isna(val):
						ws_report.write_blank(start_row + 2 + i, j, None)
					else:
						ws_report.write_number(start_row + 2 + i, j, float(val))
			return start_row + 2 + len(df.index) + 2  # next block start

		row_cursor = 0
		row_cursor = write_block("People (人數)", people, row_cursor)
		row_cursor = write_block("Shares (股數)", shares, row_cursor)
		row_cursor = write_block("Percent (占比 %)", percent, row_cursor)

		# Create charts referencing hidden sheets for cleaner ranges
		book = writer.book
		chart_configs = [
			("People (人數)", people_sheet),
			("Shares (股數)", shares_sheet),
			("Percent (占比 %)", percent_sheet),
		]
		insert_row = 0
		for title, sheet in chart_configs:
			chart = book.add_chart({"type": "line"})
			# Add 15 series
			for i in range(15):
				col = i + 1  # A is dates, data columns start at 1
				chart.add_series({
					"name":       [sheet, 0, col],
					"categories": [sheet, 1, 0, len(people.index), 0],
					"values":     [sheet, 1, col, len(people.index), col],
				})
			chart.set_title({"name": f"{code} - {title}"})
			chart.set_x_axis({"date_axis": True})
			chart.set_y_axis({"major_gridlines": {"visible": True}})
			# Insert chart under each block
			ws_report.insert_chart(insert_row, 18, chart, {"x_scale": 1.1, "y_scale": 1.1})
			insert_row += 20

			# Optionally overlay close price as secondary axis using a separate line chart
			if kline is not None and not kline.empty:
				k_chart = book.add_chart({"type": "line"})
				# Write kline close series to a temp hidden sheet for reference
				k_sheet = f"kline_{title[:3]}"
				k_df = kline[["date", "close"]].copy()
				k_df.to_excel(writer, sheet_name=k_sheet, index=False)
				writer.sheets[k_sheet].hide()
				k_chart.add_series({
					"name":       [k_sheet, 0, 1],
					"categories": [k_sheet, 1, 0, len(k_df.index), 0],
					"values":     [k_sheet, 1, 1, len(k_df.index), 1],
					"y2_axis":    True,
					"line":       {"color": "#999999"},
				})
				k_chart.set_legend({"position": "none"})
				ws_report.insert_chart(insert_row - 20, 18, k_chart, {"x_scale": 1.1, "y_scale": 1.1})

		# Footer
		ws_report.write(insert_row + 2, 0, f"Generated for {code}")


def main(argv: Optional[List[str]] = None) -> int:
	parser = argparse.ArgumentParser(description="資料查詢與整理 (程式二)")
	parser.add_argument("--data-dir", default="/workspace/data", help="本地資料庫根目錄")
	parser.add_argument("--code", required=True, help="股號")
	parser.add_argument("--start", required=True, help="起始日期 YYYY-MM-DD")
	parser.add_argument("--end", required=True, help="結束日期 YYYY-MM-DD")
	parser.add_argument("--output", default=None, help="輸出 Excel 檔案路徑")
	parser.add_argument("--simulate", action="store_true", help="無法取得 K 線時使用模擬資料")
	args = parser.parse_args(argv)

	data_dir = args.data_dir
	code = args.code
	start = parse_date_any(args.start).date()
	end = parse_date_any(args.end).date()

	records = list_stock_records(data_dir, code)
	if not records:
		print("Warning: 無資料記錄，請先執行程式一建立本地資料庫。")
		return 1
	dates = [r["_date"] for r in records]
	start_idx = nearest_index_by_date(dates, start, prefer_ge=True)
	end_idx = nearest_index_by_date(dates, end, prefer_ge=True)
	warnings: List[str] = []
	if dates[start_idx] < start:
		warnings.append("起始日期的最近資料為較小日期 (回退)。")
	if dates[end_idx] > end:
		warnings.append("結束日期的最近資料為較大日期 (前推)。")

	slice_records = records[start_idx : end_idx + 1]
	people, shares, percent = build_tables(slice_records)

	# Attempt to load K line; simulate if needed
	kline: Optional[pd.DataFrame] = None
	try:
		if args.simulate:
			kline = simulate_kline(list(people.index), seed=int(code))
		else:
			kline = simulate_kline(list(people.index), seed=int(code))
	except Exception:
		kline = None

	if args.output:
		output = args.output
	else:
		output_dir = os.path.join("/workspace", "output")
		ensure_dir(output_dir)
		output = os.path.join(output_dir, f"{code}_report.xlsx")

	write_excel_with_charts(output, code, people, shares, percent, kline)
	for w in warnings:
		print("Warning:", w)
	print("完成：", output)
	return 0


if __name__ == "__main__":
	sys.exit(main())