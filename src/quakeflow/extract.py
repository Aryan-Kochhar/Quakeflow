"""Extract: pull GeoJSON features from the USGS FDSN event API."""

from __future__ import annotations

import gzip
import json
import logging
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

Fetcher = Callable[[datetime, datetime], list[dict]]

_TS_FORMAT = "%Y-%m-%dT%H:%M:%S"


def make_session() -> requests.Session:
    """HTTP session with exponential backoff on throttling and transient server errors."""
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "quakeflow/0.1 (portfolio data pipeline)"
    return session


class UsgsClient:
    def __init__(
        self,
        api_url: str,
        page_limit: int = 20_000,
        session: requests.Session | None = None,
        timeout: float = 60,
    ) -> None:
        self.api_url = api_url
        self.page_limit = page_limit
        self.session = session or make_session()
        self.timeout = timeout

    def fetch(self, start: datetime, end: datetime) -> list[dict]:
        """Fetch all events in [start, end]. Times are naive UTC."""
        params = {
            "format": "geojson",
            "starttime": start.strftime(_TS_FORMAT),
            "endtime": end.strftime(_TS_FORMAT),
            "limit": self.page_limit,
            "orderby": "time-asc",
        }
        resp = self.session.get(self.api_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()["features"]


def windows(
    start: datetime, end: datetime, step: timedelta = timedelta(days=1)
) -> Iterator[tuple[datetime, datetime]]:
    """Split [start, end) into consecutive windows of at most `step`."""
    cursor = start
    while cursor < end:
        upper = min(cursor + step, end)
        yield cursor, upper
        cursor = upper


def fetch_window(
    fetch: Fetcher,
    start: datetime,
    end: datetime,
    page_limit: int,
    min_window: timedelta = timedelta(minutes=1),
) -> list[dict]:
    """Fetch a window, bisecting it whenever the API returns a full page.

    A full page means results were probably truncated, so the window is halved
    until every sub-window fits. Boundary events may appear twice; the loader
    deduplicates on event id.
    """
    features = fetch(start, end)
    if len(features) < page_limit:
        return features
    if end - start <= min_window:
        raise RuntimeError(f"{start}..{end} still exceeds {page_limit} events at minimum window")
    mid = start + (end - start) / 2
    log.info("window %s..%s hit page limit, splitting at %s", start, end, mid)
    return fetch_window(fetch, start, mid, page_limit, min_window) + fetch_window(
        fetch, mid, end, page_limit, min_window
    )


def land(features: list[dict], landing_dir: Path, window_start: datetime, run_id: str) -> Path:
    """Persist the untouched API payload so any load can be replayed or audited later."""
    partition = landing_dir / "usgs" / f"date={window_start:%Y-%m-%d}"
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"{run_id}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump(features, fh)
    return path
