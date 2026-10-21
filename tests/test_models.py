from datetime import datetime

from conftest import make_feature

from quakeflow.models import Event, parse_features

T = datetime(2026, 10, 1, 12, 30)


def test_feature_maps_to_typed_event():
    event = Event.from_feature(make_feature("hv1", T, mag=4.6, depth=12.5))
    assert event.event_id == "hv1"
    assert event.event_time == T
    assert event.event_time.tzinfo is None  # naive UTC
    assert event.magnitude == 4.6
    assert (event.latitude, event.longitude, event.depth_km) == (19.4, -155.3, 12.5)
    assert event.tsunami is False


def test_null_magnitude_is_allowed():
    assert Event.from_feature(make_feature("hv1", T, mag=None)).magnitude is None


def test_bad_records_are_quarantined_not_raised():
    good = make_feature("ok", T)
    bad_lat = make_feature("bad_lat", T, lat=123.0)
    no_geometry = make_feature("no_geo", T)
    del no_geometry["geometry"]
    bad_mag = make_feature("bad_mag", T, mag=42)

    events, rejected = parse_features([good, bad_lat, no_geometry, bad_mag])

    assert [e.event_id for e in events] == ["ok"]
    assert {r.event_id for r in rejected} == {"bad_lat", "no_geo", "bad_mag"}
    assert all(r.error and r.payload for r in rejected)
