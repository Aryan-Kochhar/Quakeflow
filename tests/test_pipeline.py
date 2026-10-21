from datetime import datetime, timedelta

import duckdb
import pytest
from conftest import fake_source, make_feature

from quakeflow import load
from quakeflow.pipeline import resolve_window, run_ingest

START = datetime(2026, 10, 1)

FEATURES = [
    make_feature("a", START + timedelta(hours=3)),
    make_feature("b", START + timedelta(days=1, hours=5), mag=5.1, place="Fiji region"),
    make_feature("bad", START + timedelta(hours=4), lat=500),
]


def test_ingest_loads_quarantines_and_records_run(settings):
    result = run_ingest(settings, START, START + timedelta(days=2), fetch=fake_source(FEATURES))

    assert result.fetched == 3
    assert result.stats.inserted == 2
    assert result.quarantined == 1
    with duckdb.connect(str(settings.db_path)) as con:
        assert con.execute("select count(*) from raw.usgs_quarantine").fetchone()[0] == 1
        status, window_end = con.execute(
            "select status, window_end from raw.pipeline_runs"
        ).fetchone()
    assert (status, window_end) == ("success", START + timedelta(days=2))
    assert len(list(settings.landing_dir.rglob("*.json.gz"))) == 2


def test_rerun_is_idempotent(settings):
    end = START + timedelta(days=2)
    run_ingest(settings, START, end, fetch=fake_source(FEATURES))
    again = run_ingest(settings, START, end, fetch=fake_source(FEATURES))
    assert again.stats.inserted == 0 and again.stats.unchanged == 2


def test_failed_run_does_not_advance_watermark(settings):
    def broken(s, e):
        raise ConnectionError("USGS down")

    with pytest.raises(ConnectionError):
        run_ingest(settings, START, START + timedelta(days=1), fetch=broken)

    with duckdb.connect(str(settings.db_path)) as con:
        assert load.last_successful_watermark(con) is None
        assert con.execute("select status from raw.pipeline_runs").fetchone()[0] == "failed"


def test_window_resolution(settings):
    con = load.connect(settings.db_path)
    now = datetime(2026, 10, 10, 8, 15, 42)

    # First run: backfill
    assert resolve_window(con, settings, now=now) == (
        datetime(2026, 10, 8, 8, 15),
        datetime(2026, 10, 10, 8, 15),
    )
    # Later runs: watermark minus lookback
    load.start_run(con, "r1", now, datetime(2026, 10, 1), datetime(2026, 10, 9))
    load.finish_run(con, "r1", now, "success", 0, load.LoadStats(), 0)
    assert resolve_window(con, settings, now=now)[0] == datetime(2026, 10, 8)
