"""Command-line entry point: `quakeflow ingest | transform | run | status`."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

import duckdb

from quakeflow.config import Settings
from quakeflow.pipeline import run_ingest, run_transform


def _date(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _ingest(args: argparse.Namespace, settings: Settings) -> None:
    r = run_ingest(settings, start=args.start, end=args.end)
    print(
        f"run {r.run_id}: {r.window_start} -> {r.window_end} | fetched {r.fetched}, "
        f"inserted {r.stats.inserted}, updated {r.stats.updated}, "
        f"unchanged {r.stats.unchanged}, quarantined {r.quarantined}"
    )


def _status(settings: Settings) -> None:
    if not settings.db_path.exists():
        print(f"No warehouse at {settings.db_path}. Run `quakeflow run` first.")
        return
    with duckdb.connect(str(settings.db_path), read_only=True) as con:
        con.sql(
            """
            select run_id, status, window_start, window_end, fetched, inserted, updated,
                   quarantined, date_diff('second', started_at, finished_at) as secs
            from raw.pipeline_runs order by started_at desc limit 10
            """
        ).show()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="quakeflow", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_ in [
        ("ingest", "extract + load raw events"),
        ("run", "ingest, then dbt build"),
    ]:
        p = sub.add_parser(name, help=help_)
        p.add_argument(
            "--start", type=_date, help="UTC start (YYYY-MM-DD[THH:MM]); default: watermark"
        )
        p.add_argument("--end", type=_date, help="UTC end; default: now")
    sub.add_parser("transform", help="dbt build (models + tests)")
    sub.add_parser("status", help="show recent pipeline runs")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()

    if args.command == "ingest":
        _ingest(args, settings)
    elif args.command == "transform":
        run_transform(settings)
    elif args.command == "run":
        _ingest(args, settings)
        run_transform(settings)
    elif args.command == "status":
        _status(settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
