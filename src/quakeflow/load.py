"""Load: idempotent upserts into the DuckDB raw layer, plus run bookkeeping."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from quakeflow.models import Event, Rejected

_DDL = """
create schema if not exists raw;

create table if not exists raw.usgs_events (
    event_id        varchar primary key,
    event_time      timestamp not null,
    updated_at      timestamp not null,
    magnitude       double,
    magnitude_type  varchar,
    place           varchar,
    event_type      varchar not null,
    status          varchar,
    tsunami         boolean not null,
    significance    integer,
    felt_reports    integer,
    alert_level     varchar,
    network         varchar,
    longitude       double not null,
    latitude        double not null,
    depth_km        double,
    url             varchar,
    ingested_at     timestamp not null,
    run_id          varchar not null
);

create table if not exists raw.usgs_quarantine (
    run_id          varchar not null,
    event_id        varchar,
    error           varchar not null,
    payload         varchar not null,
    quarantined_at  timestamp not null
);

create table if not exists raw.pipeline_runs (
    run_id          varchar primary key,
    started_at      timestamp not null,
    finished_at     timestamp,
    window_start    timestamp not null,
    window_end      timestamp not null,
    status          varchar not null,
    fetched         integer default 0,
    inserted        integer default 0,
    updated         integer default 0,
    unchanged       integer default 0,
    quarantined     integer default 0,
    error           varchar
);
"""

_EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_time", pa.timestamp("us")),
        ("updated_at", pa.timestamp("us")),
        ("magnitude", pa.float64()),
        ("magnitude_type", pa.string()),
        ("place", pa.string()),
        ("event_type", pa.string()),
        ("status", pa.string()),
        ("tsunami", pa.bool_()),
        ("significance", pa.int32()),
        ("felt_reports", pa.int32()),
        ("alert_level", pa.string()),
        ("network", pa.string()),
        ("longitude", pa.float64()),
        ("latitude", pa.float64()),
        ("depth_km", pa.float64()),
        ("url", pa.string()),
    ]
)
_EVENT_COLUMNS = _EVENT_SCHEMA.names
_UPDATABLE = [c for c in _EVENT_COLUMNS if c != "event_id"] + ["ingested_at", "run_id"]


@dataclass
class LoadStats:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0

    def __iadd__(self, other: LoadStats) -> LoadStats:
        self.inserted += other.inserted
        self.updated += other.updated
        self.unchanged += other.unchanged
        return self


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(_DDL)
    return con


def upsert_events(
    con: duckdb.DuckDBPyConnection, events: list[Event], run_id: str, ingested_at: datetime
) -> LoadStats:
    """Insert new events and overwrite existing ones only when USGS has a newer revision.

    Safe to re-run with the same data: replaying a batch changes nothing.
    """
    if not events:
        return LoadStats()

    batch = pa.Table.from_pylist([e.model_dump() for e in events], schema=_EVENT_SCHEMA)
    con.register("_batch", batch)
    try:
        con.execute("begin")
        # An event can appear twice in one batch (window boundaries); keep its latest revision.
        con.execute(
            """
            create or replace temp table _staged as
            select * from _batch
            qualify row_number() over (partition by event_id order by updated_at desc) = 1
            """
        )
        inserted, updated, unchanged = con.execute(
            """
            select
                count(*) filter (where r.event_id is null),
                count(*) filter (where s.updated_at > r.updated_at),
                count(*) filter (where s.updated_at <= r.updated_at)
            from _staged s
            left join raw.usgs_events r using (event_id)
            """
        ).fetchone()

        cols = ", ".join(_EVENT_COLUMNS)
        assignments = ", ".join(f"{c} = excluded.{c}" for c in _UPDATABLE)
        con.execute(
            f"""
            insert into raw.usgs_events ({cols}, ingested_at, run_id)
            select {cols}, ?, ? from _staged
            on conflict (event_id) do update set {assignments}
            where excluded.updated_at > raw.usgs_events.updated_at
            """,
            [ingested_at, run_id],
        )
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    finally:
        con.unregister("_batch")

    return LoadStats(inserted=inserted, updated=updated, unchanged=unchanged)


def quarantine(
    con: duckdb.DuckDBPyConnection, rejected: list[Rejected], run_id: str, at: datetime
) -> None:
    if rejected:
        con.executemany(
            "insert into raw.usgs_quarantine values (?, ?, ?, ?, ?)",
            [(run_id, r.event_id, r.error, r.payload, at) for r in rejected],
        )


def last_successful_watermark(con: duckdb.DuckDBPyConnection) -> datetime | None:
    return con.execute(
        "select max(window_end) from raw.pipeline_runs where status = 'success'"
    ).fetchone()[0]


def start_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    started_at: datetime,
    start: datetime,
    end: datetime,
) -> None:
    con.execute(
        "insert into raw.pipeline_runs (run_id, started_at, window_start, window_end, status) "
        "values (?, ?, ?, ?, 'running')",
        [run_id, started_at, start, end],
    )


def finish_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    finished_at: datetime,
    status: str,
    fetched: int,
    stats: LoadStats,
    quarantined: int,
    error: str | None = None,
) -> None:
    con.execute(
        """
        update raw.pipeline_runs
        set finished_at = ?, status = ?, fetched = ?, inserted = ?, updated = ?,
            unchanged = ?, quarantined = ?, error = ?
        where run_id = ?
        """,
        [
            finished_at,
            status,
            fetched,
            stats.inserted,
            stats.updated,
            stats.unchanged,
            quarantined,
            error,
            run_id,
        ],
    )
