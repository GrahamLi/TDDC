import os
import re
import sys
import json
import math
import argparse
from datetime import date, timedelta, datetime
from typing import Dict, List, Any, Optional, Tuple

from tdcc_utils import HttpClient, ensure_dir, save_json, list_dirs, parse_date_any

MONEYDJ_STOCK_TABLE = "https://moneydj.emega.com.tw/js/StockTable.htm"
# Note: TDCC site is dynamic; the exact endpoint may change. We attempt a known AJAX endpoint.
TDCC_AJAX_ENDPOINT = "https://www.tdcc.com.tw/smWeb/QryStockAjax.do"

DEFAULT_EXCLUDE_PATTERNS = [
	"ETF",
	"反1",
	"正2",
	"槓桿",
	"反向",
	"債",
	"原油",
	"黃金",
	"富邦VIX",
]


def daterange_weekly(since: date, until: date) -> List[date]:
	# generate Mondays between since and until inclusive
	if since > until:
		return []
	start = since - timedelta(days=since.weekday())
	end = until - timedelta(days=until.weekday())
	out: List[date] = []
	cur = start
	while cur <= end:
		out.append(cur)
		cur += timedelta(days=7)
	return [d for d in out if since <= d <= until]


def parse_moneydj_stock_codes(html_text: str) -> List[Tuple[str, str]]:
	# Attempt to parse stock codes and names from MoneyDJ StockTable.htm
	# The file often contains table rows like: <tr><td>2330 台積電</td> ...
	codes: List[Tuple[str, str]] = []
	for m in re.finditer(r">(\d{4,5})\s*([^<\n\r]+)<", html_text):
		code = m.group(1)
		name = m.group(2).strip()
		codes.append((code, name))
	# Fallback: also try patterns inside JS arrays
	if not codes:
		for m in re.finditer(r"\[\'?(\d{4,5})\'?,\s*\'([^\']+)\'\]", html_text):
			codes.append((m.group(1), m.group(2)))
	return codes


def fetch_stock_codes(client: HttpClient) -> List[Tuple[str, str]]:
	resp = client.get(MONEYDJ_STOCK_TABLE)
	resp.encoding = resp.apparent_encoding or resp.encoding
	pairs = parse_moneydj_stock_codes(resp.text)
	# Deduplicate while preserving order
	seen = set()
	out: List[Tuple[str, str]] = []
	for code, name in pairs:
		if code not in seen:
			seen.add(code)
			out.append((code, name))
	return out


def should_exclude(name: str, exclude_patterns: List[str]) -> bool:
	for pat in exclude_patterns:
		if re.search(pat, name, flags=re.IGNORECASE):
			return True
	return False


def get_existing_dates(stock_dir: str) -> List[date]:
	if not os.path.isdir(stock_dir):
		return []
	dates: List[date] = []
	for fn in os.listdir(stock_dir):
		if fn.endswith(".json"):
			try:
				dates.append(parse_date_any(fn.replace(".json", "")))
			except Exception:
				continue
	return sorted(dates)


def simulate_tdcc_levels(seed: int) -> List[Dict[str, Any]]:
	# Generate a synthetic TDCC 15-level distribution that sums to 100%
	rnd = (math.sin(seed) + 1.5) * 0.5
	levels: List[Dict[str, Any]] = []
	total_holders = 100000 + int(50000 * rnd)
	total_shares = 10_000_000 + int(5_000_000 * rnd)
	remaining_percent = 100.0
	for i in range(15):
		if i < 14:
			pct = max(0.2, (5.0 + (i % 3) * 1.1) * (0.9 + (seed % 7) * 0.01) / 15.0)
			pct = min(pct, max(0.1, remaining_percent - (14 - i) * 0.1))
			remaining_percent -= pct
		else:
			pct = max(0.1, remaining_percent)
		holders = max(10, int(total_holders * pct / 100.0))
		shares = max(100, int(total_shares * pct / 100.0))
		levels.append({
			"level": f"L{i+1:02d}",
			"holderCount": holders,
			"shareCount": shares,
			"percent": pct,
		})
	return levels


def fetch_tdcc_distribution(client: HttpClient, code: str, qdate: date) -> Optional[Dict[str, Any]]:
	# Attempt to query the AJAX endpoint; if fails, return None
	payload = {
		"isDetail": "Y",
		"stockNo": code,
		"scaDate": qdate.strftime("%Y/%m/%d"),
	}
	try:
		resp = client.post(TDCC_AJAX_ENDPOINT, data=payload)
		data = resp.json()
		if not data:
			return None
		return {
			"stock": code,
			"date": qdate.isoformat(),
			"levels": [
				{
					"level": str(item.get("level", i + 1)),
					"holderCount": int(item.get("people", 0)),
					"shareCount": int(item.get("unit", 0)),
					"percent": float(item.get("percent", 0.0)),
				}
				for i, item in enumerate(data if isinstance(data, list) else data.get("data", []))
			],
		}
	except Exception:
		return None


def crawl_for_code(client: HttpClient, code: str, stock_name: str, output_dir: str, since: date, until: date, simulate: bool) -> None:
	stock_dir = os.path.join(output_dir, code)
	ensure_dir(stock_dir)
	existing = set(get_existing_dates(stock_dir))
	week_dates = daterange_weekly(since, until)
	for d in week_dates:
		if d in existing:
			continue
		if simulate:
			seed = int(d.strftime("%Y%m%d")) + int(code)
			rec = {
				"stock": code,
				"name": stock_name,
				"date": d.isoformat(),
				"levels": simulate_tdcc_levels(seed),
			}
			filepath = os.path.join(stock_dir, f"{d.isoformat()}.json")
			save_json(filepath, rec)
			continue
		rec = fetch_tdcc_distribution(client, code, d)
		if rec:
			rec["name"] = stock_name
			filepath = os.path.join(stock_dir, f"{d.isoformat()}.json")
			save_json(filepath, rec)


def main(argv: Optional[List[str]] = None) -> int:
	parser = argparse.ArgumentParser(description="TDCC 股權分佈資料爬蟲 (程式一)")
	parser.add_argument("--output-dir", default="/workspace/data", help="本地資料庫根目錄")
	parser.add_argument("--since", default=None, help="起始日期 YYYY-MM-DD，預設為一年前")
	parser.add_argument("--until", default=None, help="結束日期 YYYY-MM-DD，預設為今天")
	parser.add_argument("--codes", default=None, help="限定股號清單，逗號分隔，例如: 2330,2317")
	parser.add_argument("--max-codes", type=int, default=50, help="最多處理多少檔 (防止誤抓)")
	parser.add_argument("--simulate", action="store_true", help="使用模擬資料 (無網路時可用)")
	parser.add_argument("--exclude-patterns", default=",".join(DEFAULT_EXCLUDE_PATTERNS), help="排除名稱關鍵字，逗號分隔")
	args = parser.parse_args(argv)

	until = parse_date_any(args.until).date() if args.until else date.today()
	since = parse_date_any(args.since).date() if args.since else (until - timedelta(days=365))

	ensure_dir(args.output_dir)
	client = HttpClient()

	codes_and_names: List[Tuple[str, str]]
	if args.codes:
		codes_and_names = [(c.strip(), c.strip()) for c in args.codes.split(",") if c.strip()]
	else:
		if args.simulate:
			codes_and_names = [("2330", "台積電"), ("2317", "鴻海"), ("2603", "長榮")]
		else:
			codes_and_names = fetch_stock_codes(client)

	exclude_patterns = [s.strip() for s in args.exclude_patterns.split(",") if s.strip()]
	picked: List[Tuple[str, str]] = []
	for code, name in codes_and_names:
		if should_exclude(name, exclude_patterns):
			continue
		picked.append((code, name))
		if len(picked) >= args.max_codes:
			break

	for idx, (code, name) in enumerate(picked, start=1):
		print(f"[{idx}/{len(picked)}] 抓取 {code} {name}")
		crawl_for_code(client, code, name, args.output_dir, since, until, args.simulate)

	print("完成。")
	return 0


if __name__ == "__main__":
	sys.exit(main())