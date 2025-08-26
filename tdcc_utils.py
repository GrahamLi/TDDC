import os
import json
import time
import random
from typing import Any, Dict, Optional, List
import requests
from requests import Response
from dateutil import parser as dateparser

DEFAULT_HEADERS = {
	"User-Agent": (
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
		"(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
	),
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	"Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

class HttpClient:
	def __init__(self, max_retries: int = 3, timeout_seconds: int = 20):
		self.session = requests.Session()
		self.session.headers.update(DEFAULT_HEADERS)
		self.max_retries = max_retries
		self.timeout_seconds = timeout_seconds

	def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Response:
		return self._request("GET", url, params=params)

	def post(self, url: str, data: Optional[Dict[str, Any]] = None) -> Response:
		return self._request("POST", url, data=data)

	def _request(self, method: str, url: str, params: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None) -> Response:
		last_exc: Optional[Exception] = None
		for attempt in range(1, self.max_retries + 1):
			try:
				if method == "GET":
					resp = self.session.get(url, params=params, timeout=self.timeout_seconds)
				else:
					resp = self.session.post(url, data=data, timeout=self.timeout_seconds)
				resp.raise_for_status()
				return resp
			except Exception as exc:  # noqa: BLE001
				last_exc = exc
				sleep_secs = min(5.0, 0.5 * attempt) + random.random() * 0.5
				time.sleep(sleep_secs)
		raise RuntimeError(f"HTTP {method} {url} failed after {self.max_retries} attempts: {last_exc}")


def ensure_dir(path: str) -> None:
	os.makedirs(path, exist_ok=True)


def save_json(filepath: str, obj: Any) -> None:
	ensure_dir(os.path.dirname(filepath) or ".")
	with open(filepath, "w", encoding="utf-8") as f:
		json.dump(obj, f, indent=2, ensure_ascii=False)


def load_json(filepath: str) -> Any:
	with open(filepath, "r", encoding="utf-8") as f:
		return json.load(f)


def list_dirs(path: str) -> List[str]:
	try:
		return [p for p in os.listdir(path) if os.path.isdir(os.path.join(path, p))]
	except FileNotFoundError:
		return []


def parse_date_any(date_text: str):
	return dateparser.parse(date_text).date()