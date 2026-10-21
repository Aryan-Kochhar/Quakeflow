from datetime import datetime, timedelta

from conftest import make_feature

from quakeflow import load
from quakeflow.models import Event

T = datetime(2026, 10, 1, 12)
NOW = datetime(2026, 10, 2)


def ev(event_id, mag=3.0, updated=T):
    return Event.from_feature(make_feature(event_id, T, mag=mag, updated=updated))


def rows(con):
    return con.execute(
        "select event_id, magnitude, run_id from raw.usgs_events order by event_id"
    ).fetchall()


def test_replaying_a_batch_is_a_no_op(tmp_path):
    con = load.connect(tmp_path / "w.duckdb")
    batch = [ev("a"), ev("b")]

    first = load.upsert_events(con, batch, "r1", NOW)
    second = load.upsert_events(con, batch, "r2", NOW)

    assert (first.inserted, first.updated) == (2, 0)
    assert (second.inserted, second.updated, second.unchanged) == (0, 0, 2)
    assert rows(con) == [("a", 3.0, "r1"), ("b", 3.0, "r1")]


def test_newer_revision_overwrites_older_does_not(tmp_path):
    con = load.connect(tmp_path / "w.duckdb")
    load.upsert_events(con, [ev("a", mag=3.0, updated=T)], "r1", NOW)

    revised = load.upsert_events(con, [ev("a", mag=3.4, updated=T + timedelta(hours=2))], "r2", NOW)
    stale = load.upsert_events(con, [ev("a", mag=9.9, updated=T + timedelta(hours=1))], "r3", NOW)

    assert revised.updated == 1 and stale.unchanged == 1
    assert rows(con) == [("a", 3.4, "r2")]


def test_duplicates_within_one_batch_keep_latest(tmp_path):
    con = load.connect(tmp_path / "w.duckdb")
    stats = load.upsert_events(
        con, [ev("a", mag=2.0), ev("a", mag=2.5, updated=T + timedelta(minutes=5))], "r1", NOW
    )
    assert stats.inserted == 1
    assert rows(con) == [("a", 2.5, "r1")]
