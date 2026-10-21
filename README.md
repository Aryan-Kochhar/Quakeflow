# QuakeFlow 🌍

**An incremental ELT pipeline for live global earthquake data.** It pulls events from the
USGS API, validates them, and loads them idempotently into a DuckDB warehouse. dbt then models
the data into tested analytics marts, and a Streamlit dashboard shows the results.

```
USGS FDSN API ──► Python extract ──► landing zone (gzip JSON, partitioned by date)
                      │
                      ▼
               pydantic validation ──► raw.usgs_quarantine  (bad records, never dropped silently)
                      │
                      ▼
               DuckDB raw.usgs_events  (upsert on event_id, newest revision wins)
                      │   raw.pipeline_runs (audit log + watermark)
                      ▼
               dbt: staging ──► fct_earthquakes (incremental) ──► marts ──► Streamlit
                     + seeds, generic & singular data tests, source freshness
```

## Why this project is interesting

Real-world earthquake data is messy in ways that toy datasets are not. The pipeline is built
around those problems:

| Problem | How QuakeFlow handles it |
|---|---|
| **USGS revises events for days** (magnitude, location, review status) | Every incremental run re-reads a lookback window. The upsert overwrites a row only when `updated_at` is newer, so late revisions land and stale replays are ignored. |
| **The API caps each query at 20,000 events** | `fetch_window` bisects any window that returns a full page until every sub-window fits, so busy periods are never silently truncated. |
| **Re-runs and partial failures** | Loads are idempotent (replaying a batch is a no-op). The watermark advances only after a fully successful run, so a failed run is retried automatically. |
| **Malformed records** | Pydantic validates every feature. Bad records go to a quarantine table with the error and the raw payload instead of crashing the run or vanishing. |
| **Reproducibility** | The untouched API response is written to a date-partitioned landing zone before it is parsed, so any load can be replayed or audited. |
| **Incremental modelling with late data** | `fct_earthquakes` is a dbt incremental model keyed on `ingested_at`, not `event_time`, so backfills and revisions flow through without a full refresh. |
| **Trusting the numbers** | 20+ dbt data tests: uniqueness, ranges, accepted values, a fact-to-mart **reconciliation** test, and a "no future events" check for time zone bugs. |

### Results from a live run

- The first backfill (30 days) fetched and loaded **10,029 real events in about 27 seconds**.
  The incremental re-run reprocessed its 3-day lookback window: 727 events, 0 duplicates.
- On real data, the `accepted_values` test flagged a record with status `REVIEWED`. USGS
  sometimes sends inconsistent casing. The staging model now normalises it. This is the kind
  of problem data tests exist to catch.
- Panama alone released **~79% of all seismic energy worldwide** that month, driven by a single
  M7.7 earthquake. Magnitude is logarithmic, so one large event outweighs thousands of small ones.
  The figure comes from `mart_region_activity`.

## Stack

Python 3.12 · DuckDB · dbt-core + dbt-duckdb · pydantic · PyArrow · Streamlit + Plotly ·
pytest · ruff · Docker · GitHub Actions

## Quickstart

```bash
python -m venv .venv
.venv\Scripts\activate          # macOS/Linux: source .venv/bin/activate
pip install -e ".[dashboard,dev]"

quakeflow run                   # backfill last 30 days, then dbt build (models + tests)
quakeflow status                # recent runs: fetched / inserted / updated / quarantined
streamlit run dashboard/app.py  # http://localhost:8501
```

Later `quakeflow run` calls are incremental: they resume from the last successful watermark minus
a 3-day lookback. For an explicit backfill:

```bash
quakeflow ingest --start 2025-01-01 --end 2025-02-01
quakeflow transform
```

With Docker:

```bash
docker compose up --build       # runs the pipeline, then serves the dashboard on :8501
```

### Configuration

| Env var | Default | Purpose |
|---|---|---|
| `QUAKEFLOW_DATA_DIR` | `./data` | Warehouse and landing zone location |
| `QUAKEFLOW_DB` | `$DATA_DIR/warehouse.duckdb` | DuckDB file |
| `QUAKEFLOW_BACKFILL_DAYS` | `30` | History pulled on the first run |
| `QUAKEFLOW_LOOKBACK_DAYS` | `3` | Re-read window to capture USGS revisions |

## Project layout

```
src/quakeflow/
  extract.py    USGS client (retries + backoff), window bisection, landing zone
  models.py     pydantic schema, quarantine of invalid records
  load.py       DuckDB DDL, idempotent upsert, run audit log
  pipeline.py   watermark resolution, run orchestration, dbt invocation
  cli.py        `quakeflow ingest | transform | run | status`
transform/      dbt project
  models/staging/   stg_usgs__events (region normalisation via seed)
  models/marts/     fct_earthquakes (incremental), mart_daily_activity, mart_region_activity
  macros/           custom generic test `value_between`, schema naming
  tests/            singular tests: reconciliation, no future events
dashboard/app.py    Streamlit dashboard
tests/              pytest unit tests + a dbt integration test on fixture data
```

## Data model

- **`fct_earthquakes`**: one row per earthquake. Adds magnitude band, depth class
  (shallow / intermediate / deep), energy released (Gutenberg–Richter:
  `log10 E = 1.5M + 4.8`), and an `is_notable` flag.
- **`mart_daily_activity`**: daily counts (all, M2.5+, M4.5+), average and max magnitude,
  total energy, and tsunami flags.
- **`mart_region_activity`**: per-region activity, ranked by share of total energy released.

## Testing

```bash
pytest -v               # unit tests + dbt integration test (no network needed)
ruff check . && ruff format --check .
```

CI runs lint and tests on every push. A scheduled job also runs the full pipeline daily
against the live USGS API as a smoke test.

## Known limitations and next steps

- Events that USGS deletes stop appearing in the feed. They are not yet tombstoned, so they stay
  in the warehouse. A fix would compare each lookback window against the API to detect deletions.
- DuckDB allows only one writer at a time, so the dashboard reads in read-only mode and should not
  be open during a write. A multi-user deployment would move the warehouse to Postgres,
  MotherDuck or BigQuery. Only the dbt profile would change.
- Scheduling uses cron or GitHub Actions. Orchestrators such as Airflow or Dagster would add
  retries per window and backfill UIs.
