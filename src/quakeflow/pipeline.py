"""Orchestration: ingest a time range end to end, then hand off to dbt for transforms."""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import duckdb

from quakeflow import load
from quakeflow.config import Settings
from quakeflow.extract import Fetcher, UsgsClient, fetch_window, land, windows
from quakeflow.models import parse_features

log = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass
class RunResult:
    run_id: str
    window_start: datetime
    window_end: datetime
    fetched: int = 0
    quarantined: int = 0
    stats: load.LoadStats = field(default_factory=load.LoadStats)


def resolve_window(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    start: datetime | None = None,
    end: datetime | None = None,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Explicit bounds win; otherwise resume from the last successful watermark minus lookback."""
    end = end or (now or utcnow()).replace(second=0, microsecond=0)
    if start is None:
        watermark = load.last_successful_watermark(con)
        if watermark is None:
            start = end - timedelta(days=settings.initial_backfill_days)
        else:
            start = watermark - timedelta(days=settings.lookback_days)
    if start >= end:
        raise ValueError(f"empty window: {start} >= {end}")
    return start, end


def run_ingest(
    settings: Settings,
    start: datetime | None = None,
    end: datetime | None = None,
    fetch: Fetcher | None = None,
) -> RunResult:
    fetch = fetch or UsgsClient(settings.api_url, settings.page_limit).fetch
    run_id = uuid.uuid4().hex[:12]
    con = load.connect(settings.db_path)
    try:
        start, end = resolve_window(con, settings, start, end)
        result = RunResult(run_id, start, end)
        load.start_run(con, run_id, utcnow(), start, end)
        log.info("run %s: ingesting %s -> %s", run_id, start, end)

        try:
            for w_start, w_end in windows(start, end):
                features = fetch_window(fetch, w_start, w_end, settings.page_limit)
                land(features, settings.landing_dir, w_start, run_id)
                events, rejected = parse_features(features)
                now = utcnow()
                result.stats += load.upsert_events(con, events, run_id, now)
                load.quarantine(con, rejected, run_id, now)
                result.fetched += len(features)
                result.quarantined += len(rejected)
                log.info(
                    "  %s: %d fetched, %d quarantined",
                    f"{w_start:%Y-%m-%d}",
                    len(features),
                    len(rejected),
                )
        except Exception as exc:
            # Rows already loaded stay (upserts are idempotent); the watermark does not
            # advance, so the next run retries the whole window.
            load.finish_run(
                con,
                run_id,
                utcnow(),
                "failed",
                result.fetched,
                result.stats,
                result.quarantined,
                f"{type(exc).__name__}: {exc}",
            )
            raise

        load.finish_run(
            con, run_id, utcnow(), "success", result.fetched, result.stats, result.quarantined
        )
        return result
    finally:
        con.close()


def run_transform(settings: Settings, command: str = "build") -> None:
    """Run dbt in-process. The ingest connection must be closed first (DuckDB is single-writer)."""
    from dbt.cli.main import dbtRunner

    os.environ["QUAKEFLOW_DB"] = str(settings.db_path)
    project = str(settings.dbt_project_dir)
    res = dbtRunner().invoke([command, "--project-dir", project, "--profiles-dir", project])
    if not res.success:
        raise RuntimeError(f"dbt {command} failed") from res.exception
