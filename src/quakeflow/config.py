"""Runtime settings, resolved from environment variables with sensible local defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path
    landing_dir: Path
    dbt_project_dir: Path
    api_url: str = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    # USGS caps a single query at 20,000 events; windows that hit the cap are split.
    page_limit: int = 20_000
    # How far back the very first run reaches.
    initial_backfill_days: int = 30
    # USGS revises events (magnitude, location, status) for days after they occur,
    # so each incremental run re-reads this many days before the watermark.
    lookback_days: int = 3

    @classmethod
    def from_env(cls) -> Settings:
        data_dir = Path(os.environ.get("QUAKEFLOW_DATA_DIR", Path.cwd() / "data")).resolve()
        return cls(
            db_path=Path(os.environ.get("QUAKEFLOW_DB", data_dir / "warehouse.duckdb")).resolve(),
            landing_dir=data_dir / "landing",
            dbt_project_dir=Path(
                os.environ.get("QUAKEFLOW_DBT_DIR", Path.cwd() / "transform")
            ).resolve(),
            initial_backfill_days=int(os.environ.get("QUAKEFLOW_BACKFILL_DAYS", 30)),
            lookback_days=int(os.environ.get("QUAKEFLOW_LOOKBACK_DAYS", 3)),
        )
