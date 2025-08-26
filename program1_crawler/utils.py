"""Utility helpers for the crawler."""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict

import requests

logger = logging.getLogger(__name__)


def request_with_retry(
    method: str,
    url: str,
    *,
    max_retries: int = 3,
    backoff: float = 1.0,
    verify: bool | str = True,
    **kwargs: Any,
) -> requests.Response:
    """Perform an HTTP request with basic retry logic.

    Parameters mirror :func:`requests.request`. Retries are attempted on any
    :class:`requests.RequestException`.

    The *verify* parameter matches the behaviour of the same argument in
    :func:`requests.request`, allowing the caller to provide a custom CA bundle
    path or disable verification.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.request(method, url, timeout=30, verify=verify, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:  # pragma: no cover - network dependent
            logger.warning("Request failed (%s/%s): %s", attempt, max_retries, exc)
            if attempt == max_retries:
                raise
            time.sleep(backoff * attempt)


def save_json(path: str, data: Dict[str, Any]) -> None:
    """Persist *data* to *path* in UTF-8 encoded JSON format."""
    import json
    from pathlib import Path

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
