import gzip
import json
from datetime import datetime, timedelta

import pytest

from quakeflow.extract import fetch_window, land, windows


def test_windows_cover_range_without_gaps():
    start, end = datetime(2026, 10, 1), datetime(2026, 10, 3, 6)
    ws = list(windows(start, end))
    assert ws[0][0] == start and ws[-1][1] == end
    assert all(a[1] == b[0] for a, b in zip(ws, ws[1:], strict=False))
    assert len(ws) == 3


def test_full_page_triggers_bisection():
    # 1 event per minute; page limit 50 forces a 2h window to be split repeatedly.
    start = datetime(2026, 10, 1)
    all_events = [start + timedelta(minutes=i) for i in range(120)]
    calls = []

    def fake_fetch(s, e):
        calls.append((s, e))
        return [{"t": t} for t in all_events if s <= t < e][:50]

    got = fetch_window(fake_fetch, start, start + timedelta(hours=2), page_limit=50)
    assert sorted(f["t"] for f in got) == all_events
    assert len(calls) > 1


def test_bisection_gives_up_at_min_window():
    with pytest.raises(RuntimeError):
        fetch_window(lambda s, e: [{}] * 10, datetime(2026, 1, 1), datetime(2026, 1, 2), 10)


def test_land_writes_replayable_gzip(tmp_path):
    path = land([{"id": "a"}], tmp_path, datetime(2026, 10, 1), "run1")
    assert path.parent.name == "date=2026-10-01"
    with gzip.open(path, "rt") as fh:
        assert json.load(fh) == [{"id": "a"}]
