"""Integration test: load fixture data, run every dbt model and test, check the marts."""

from datetime import datetime, timedelta

import duckdb
import pytest
from conftest import fake_source, make_feature

from quakeflow.pipeline import run_ingest, run_transform

START = datetime(2026, 10, 1)


@pytest.mark.integration
def test_dbt_build_produces_consistent_marts(settings):
    features = [
        make_feature("a", START + timedelta(hours=1), mag=2.1, place="5 km N of Ridgecrest, CA"),
        make_feature("b", START + timedelta(hours=2), mag=6.3, place="Fiji region", depth=550),
        make_feature("c", START + timedelta(days=1), mag=4.7, place="80 km W of Lima, Peru"),
        make_feature("blast", START + timedelta(hours=3), event_type="quarry blast"),
    ]
    run_ingest(settings, START, START + timedelta(days=2), fetch=fake_source(features))

    run_transform(settings)  # raises if any model or data test fails

    # dbt-duckdb keeps its in-process handle open, so match its (read-write) configuration.
    with duckdb.connect(str(settings.db_path)) as con:
        fact = dict(
            con.execute(
                "select event_id, region || '|' || magnitude_band || '|' || depth_class "
                "from marts.fct_earthquakes"
            ).fetchall()
        )
        daily = con.execute(
            "select event_date, earthquakes, m45_plus from marts.mart_daily_activity order by 1"
        ).fetchall()
        top = con.execute(
            "select region from marts.mart_region_activity where energy_rank = 1"
        ).fetchone()[0]

    assert fact == {
        "a": "California|micro|shallow",
        "b": "Fiji region|strong|deep",
        "c": "Peru|light|shallow",
    }  # quarry blast excluded
    assert daily == [(START.date(), 2, 1), ((START + timedelta(days=1)).date(), 1, 1)]
    assert top == "Fiji region"
