import os
import sys
import argparse
from typing import List, Optional, Tuple

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from tdcc_utils import ensure_dir

TDCC_LEVEL_SHARE_BOUNDS = [
	(1, 999),
	(1000, 5000),
	(5001, 10000),
	(10001, 15000),
	(15001, 20000),
	(20001, 30000),
	(30001, 40000),
	(40001, 50000),
	(50001, 100000),
	(100001, 200000),
	(200001, 400000),
	(400001, 600000),
	(600001, 800000),
	(800001, 1000000),
	(1000001, None),
]


def read_metric_from_excel(inp: str, sheet: str) -> pd.DataFrame:
	# Hidden or visible sheets both ok
	df = pd.read_excel(inp, sheet_name=sheet, index_col=0)
	# Ensure datetime index
	df.index = pd.to_datetime(df.index)
	return df


def dynamic_axis_limits(values: np.ndarray) -> Tuple[float, float]:
	finite = values[np.isfinite(values)]
	if finite.size == 0:
		return 0.0, 1.0
	vmin = float(np.min(finite))
	vmax = float(np.max(finite))
	if vmax - vmin <= 0:
		return vmin - 1.0, vmax + 1.0
	# Add small margins
	margin = (vmax - vmin) * 0.08
	return vmin - margin, vmax + margin


def plot_metric_lines(df: pd.DataFrame, title: str, outfile: str) -> None:
	plt.figure(figsize=(12, 6))
	for col in df.columns:
		plt.plot(df.index, df[col], label=col, linewidth=1.2)
	ymin, ymax = dynamic_axis_limits(df.values)
	plt.ylim(ymin, ymax)
	plt.title(title)
	plt.xlabel("Date")
	plt.legend(loc="upper left", ncol=3, fontsize=8)
	plt.grid(True, alpha=0.3)
	ensure_dir(os.path.dirname(outfile) or ".")
	plt.tight_layout()
	plt.savefig(outfile)
	plt.close()


def export_plot_to_excel(image_path: str, excel_path: str, title: str) -> None:
	ensure_dir(os.path.dirname(excel_path) or ".")
	with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
		ws = writer.book.add_worksheet("chart")
		writer.sheets["chart"] = ws
		ws.write(0, 0, title)
		ws.insert_image(2, 0, image_path, {"x_scale": 1.0, "y_scale": 1.0})


def main(argv: Optional[List[str]] = None) -> int:
	parser = argparse.ArgumentParser(description="數據分析與繪圖 (程式三)")
	parser.add_argument("--input", required=True, help="程式二產出的 Excel 檔案")
	parser.add_argument("--output-dir", default="/workspace/output", help="輸出目錄")
	parser.add_argument("--mode", choices=["shares", "amount", "custom"], default="shares", help="分類模式")
	parser.add_argument("--price", type=float, default=None, help="股價 (金額模式時需提供)")
	parser.add_argument("--custom-bins", default=None, help="自訂級距，格式如: 0-30000,30001-100000,100001-400000,400001-")
	args = parser.parse_args(argv)

	# Load metrics from input Excel; rely on hidden sheets if present
	people = read_metric_from_excel(args.input, "people")
	shares = read_metric_from_excel(args.input, "shares")
	percent = read_metric_from_excel(args.input, "percent")

	# For mode handling, we keep 15 lines per metric as-is (levels). Advanced regrouping can be added.
	out_dir = args.output_dir
	ensure_dir(out_dir)

	img1 = os.path.join(out_dir, "people_trend.png")
	img2 = os.path.join(out_dir, "shares_trend.png")
	img3 = os.path.join(out_dir, "percent_trend.png")

	plot_metric_lines(people, "People (人數) 趨勢 — 15 級距", img1)
	plot_metric_lines(shares, "Shares (股數) 趨勢 — 15 級距", img2)
	plot_metric_lines(percent, "Percent (占比 %) 趨勢 — 15 級距", img3)

	export_plot_to_excel(img1, os.path.join(out_dir, "people_trend.xlsx"), "People (人數) 趨勢 — 15 級距")
	export_plot_to_excel(img2, os.path.join(out_dir, "shares_trend.xlsx"), "Shares (股數) 趨勢 — 15 級距")
	export_plot_to_excel(img3, os.path.join(out_dir, "percent_trend.xlsx"), "Percent (占比 %) 趨勢 — 15 級距")

	print("完成：", out_dir)
	return 0


if __name__ == "__main__":
	sys.exit(main())