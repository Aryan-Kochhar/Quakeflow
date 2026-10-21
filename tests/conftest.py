from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from quakeflow.config import Settings


def epoch_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=UTC).timestamp() * 1000)


def make_feature(
    event_id: str,
    time: datetime,
    *,
    mag: float | None = 3.2,
    updated: datetime | None = None,
    place: str = "12 km SSW of Volcano, Hawaii",
    lat: float = 19.4,
    lon: float = -155.3,
    depth: float = 8.0,
    event_type: str = "earthquake",
) -> dict:
    """A GeoJSON feature shaped like the real USGS response."""
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "mag": mag,
            "place": place,
            "time": epoch_ms(time),
            "updated": epoch_ms(updated or time),
            "url": f"https://earthquake.usgs.gov/earthquakes/eventpage/{event_id}",
            "felt": None,
            "alert": None,
            "status": "reviewed",
            "tsunami": 0,
            "sig": 150,
            "net": "hv",
            "magType": "ml",
            "type": event_type,
        },
        "geometry": {"type": "Point", "coordinates": [lon, lat, depth]},
    }


def fake_source(features: list[dict]):
    """Stand-in for UsgsClient.fetch: serves fixture features whose time falls in [start, end)."""

    def fetch(start: datetime, end: datetime) -> list[dict]:
        def t(f):
            return datetime.fromtimestamp(f["properties"]["time"] / 1000, UTC).replace(tzinfo=None)

        return [f for f in features if start <= t(f) < end]

    return fetch


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "warehouse.duckdb",
        landing_dir=tmp_path / "landing",
        dbt_project_dir=Path(__file__).resolve().parents[1] / "transform",
        page_limit=100,
        initial_backfill_days=2,
        lookback_days=1,
    )
