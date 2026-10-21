"""Validate: turn raw GeoJSON features into typed records, quarantining anything malformed."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, ValidationError


def _from_epoch_ms(ms: int | float) -> datetime:
    # Stored as naive UTC so DuckDB never applies a session time zone.
    return datetime.fromtimestamp(ms / 1000, tz=UTC).replace(tzinfo=None)


class Event(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str = Field(min_length=1)
    event_time: datetime
    updated_at: datetime
    magnitude: float | None = Field(default=None, ge=-2, le=10)
    magnitude_type: str | None = None
    place: str | None = None
    event_type: str
    status: str | None = None
    tsunami: bool = False
    significance: int | None = None
    felt_reports: int | None = None
    alert_level: str | None = None
    network: str | None = None
    longitude: float = Field(ge=-180, le=180)
    latitude: float = Field(ge=-90, le=90)
    depth_km: float | None = None
    url: str | None = None

    @classmethod
    def from_feature(cls, feature: dict) -> Event:
        props = feature["properties"]
        lon, lat, depth = (list(feature["geometry"]["coordinates"]) + [None] * 3)[:3]
        return cls(
            event_id=feature["id"],
            event_time=_from_epoch_ms(props["time"]),
            updated_at=_from_epoch_ms(props["updated"]),
            magnitude=props.get("mag"),
            magnitude_type=props.get("magType"),
            place=props.get("place"),
            event_type=props["type"],
            status=props.get("status"),
            tsunami=bool(props.get("tsunami")),
            significance=props.get("sig"),
            felt_reports=props.get("felt"),
            alert_level=props.get("alert"),
            network=props.get("net"),
            longitude=lon,
            latitude=lat,
            depth_km=depth,
            url=props.get("url"),
        )


@dataclass(frozen=True)
class Rejected:
    event_id: str | None
    error: str
    payload: str


def parse_features(features: list[dict]) -> tuple[list[Event], list[Rejected]]:
    events: list[Event] = []
    rejected: list[Rejected] = []
    for feature in features:
        try:
            events.append(Event.from_feature(feature))
        except (ValidationError, KeyError, TypeError, ValueError) as exc:
            event_id = feature.get("id") if isinstance(feature, dict) else None
            rejected.append(Rejected(event_id, f"{type(exc).__name__}: {exc}", json.dumps(feature)))
    return events, rejected
