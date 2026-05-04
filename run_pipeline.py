#!/usr/bin/env python3
"""Orchestrate the full NYC TLC + POI + Weather data engineering pipeline.

This script automates the 4-stage pipeline by calling each individual script
as a subprocess in the correct order, with the right arguments and file paths.

Usage
-----
python run_pipeline.py                    # run all 4 stages
python run_pipeline.py --skip-download    # skip stage 1 (TLC already downloaded)
python run_pipeline.py --dry-run          # print commands without executing
python run_pipeline.py --dry-run --skip-download
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Directory where this script lives — used to locate the .venv
_SCRIPT_DIR = Path(__file__).parent


def _find_python() -> str:
    """Return a working Python interpreter path.

    sys.executable can point to a broken path when a venv was created on a
    different machine (e.g. a Mac Anaconda venv copied to Windows). To work
    around this, we check for the project venv first, then fall back to
    sys.executable, and finally to whatever 'python' is on PATH.
    """
    # Check for venv Python relative to this script (Windows and Unix paths)
    for rel in (".venv/Scripts/python.exe", ".venv/bin/python"):
        candidate = _SCRIPT_DIR / rel
        if candidate.exists():
            return str(candidate)
    # Fall back to the interpreter that launched this script
    if Path(sys.executable).exists():
        return sys.executable
    # Last resort: search PATH
    for name in ("python", "python3"):
        found = shutil.which(name)
        if found:
            return found
    raise RuntimeError(
        f"Cannot find a working Python interpreter (sys.executable={sys.executable!r}). "
        "Activate your virtual environment and re-run."
    )


# ── Default file paths ─────────────────────────────────────────────────────────
# These match the default paths used in each individual pipeline script,
# so no manual path configuration is needed for the standard project layout.

YEAR              = 2025

# Stage 1 — TLC taxi trip data
RAW_TLC_DIR       = "data/raw/tlc"                                      # downloaded monthly parquets
PROCESSED_TLC_DIR = "data/processed/tlc"                                # aggregated zone×date output
TLC_OUTPUT        = f"data/processed/tlc/yellow_zone_date_{YEAR}.parquet"

# Stage 2 — Points of Interest
POI_CSV           = "data/raw/poi/CommonPlace_20260408.csv"             # NYC CommonPlace export
ZONES_FILE        = "data/raw/taxi_zones/taxi_zones.shp"                # NYC taxi zone shapefile
POI_OUTPUT        = "data/processed/poi/poi_zone_features.parquet"      # zone-level POI counts

# Stage 3 — Weather
WEATHER_OUTPUT    = f"data/processed/weather/weather_{YEAR}.csv"        # NOAA daily weather

# Stage 4 — Master table
MASTER_OUTPUT     = f"data/processed/final/zone_date_master_{YEAR}.parquet"  # final joined table

WIDTH = 64  # display width for stage dividers


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the full NYC data engineering pipeline.")
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print each stage command without executing it.",
    )
    p.add_argument(
        "--skip-download", action="store_true",
        help=f"Skip stage 1 (TLC download + aggregate). "
             f"Expects {TLC_OUTPUT} to already exist.",
    )
    return p.parse_args()


def _fmt(seconds: float) -> str:
    """Format elapsed seconds as a human-readable string (e.g. '2m 31s')."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


def run_stage(n: int, total: int, label: str, cmd: list[str], dry_run: bool) -> float:
    """Run one pipeline stage as a subprocess.

    Prints the stage header, runs the command, and returns elapsed seconds.
    If the subprocess exits with a non-zero code, prints a failure message
    and calls sys.exit so the pipeline stops immediately — no further stages run.
    """
    bar = "─" * WIDTH
    print(f"\n{bar}")
    print(f"  Stage {n}/{total}: {label}")
    print(f"  Command : {' '.join(cmd)}")
    print(f"  Started : {datetime.now().strftime('%H:%M:%S')}")
    print(bar)

    # In dry-run mode, just show what would run without executing
    if dry_run:
        print("  [DRY RUN] Skipping execution.")
        return 0.0

    t0 = time.monotonic()
    result = subprocess.run(cmd)  # stdout/stderr inherit from parent so output streams live
    elapsed = time.monotonic() - t0

    # Any non-zero exit code means the stage script failed — stop the pipeline
    if result.returncode != 0:
        print(f"\n{'=' * WIDTH}")
        print(f"  PIPELINE FAILED")
        print(f"  Failed stage : {n}/{total} — {label}")
        print(f"  Exit code    : {result.returncode}")
        print(f"  Check the output above for details.")
        print(f"{'=' * WIDTH}")
        sys.exit(result.returncode)

    print(f"  Finished: {datetime.now().strftime('%H:%M:%S')}  ({_fmt(elapsed)})")
    return elapsed


def build_stages() -> list[tuple[str, list[str]]]:
    """Return the ordered list of (label, command) tuples for all 4 pipeline stages.

    Each command is a list of strings passed directly to subprocess.run,
    using the venv Python so all dependencies are available.
    """
    python = _find_python()
    return [
        # Stage 1: Download all 12 monthly TLC parquet files and aggregate them
        # into a single zone×date parquet (trips per taxi zone per day).
        (
            "Download & aggregate TLC yellow taxi data",
            [
                python, "download_tlc_2025.py",
                "--year", str(YEAR),
                "--modes", "yellow",
                "--raw-dir", RAW_TLC_DIR,
                "--processed-dir", PROCESSED_TLC_DIR,
                "--aggregate",          # also produce the aggregated zone×date output
            ],
        ),
        # Stage 2: Spatially join POI locations onto taxi zone polygons and
        # produce per-zone POI counts broken down by category.
        # --point-col: the geometry column in the CommonPlace CSV (WKT format)
        # --category-cols: columns used to classify each POI into a category
        (
            "Build POI zone features",
            [
                python, "build_poi_zone_features.py",
                "--poi-csv",       POI_CSV,
                "--zones-file",    ZONES_FILE,
                "--output-file",   POI_OUTPUT,
                "--point-col",     "the_geom",               # WKT geometry column in CommonPlace CSV
                "--category-cols", "FACILITY DOMAINS", "FACILITY TYPE",  # category columns in CommonPlace CSV
            ],
        ),
        # Stage 3: Fetch daily weather summaries from NOAA for Central Park
        # (station USW00094728) covering the full year.
        (
            "Fetch NOAA daily weather",
            [
                python, "build_weather_2025.py",
                "--start-date",  f"{YEAR}-01-01",
                "--end-date",    f"{YEAR}-12-31",
                "--output-file", WEATHER_OUTPUT,
            ],
        ),
        # Stage 4: Join TLC trip counts, POI zone features, and daily weather
        # into one master table keyed by (taxi_zone_id, date).
        (
            "Build zone × date master table",
            [
                python, "build_zone_date_master.py",
                "--tlc-files",    TLC_OUTPUT,
                "--poi-file",     POI_OUTPUT,
                "--weather-file", WEATHER_OUTPUT,
                "--output-file",  MASTER_OUTPUT,
            ],
        ),
    ]


def main() -> int:
    args = parse_args()
    all_stages = build_stages()

    # If --skip-download is set, drop stage 1 from the run list.
    # Useful when raw TLC files are already downloaded and you only want to
    # re-run the POI, weather, and master table steps.
    if args.skip_download:
        if not Path(TLC_OUTPUT).exists():
            print(f"[WARN] --skip-download set but {TLC_OUTPUT} not found.")
            print("       Stage 4 (master table) will fail unless the file exists.")
        stages = all_stages[1:]
    else:
        stages = all_stages

    # Print pipeline header
    print(f"\n{'=' * WIDTH}")
    print("  NYC Data Engineering Pipeline")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.dry_run:
        print("  Mode : DRY RUN (no commands will execute)")
    if args.skip_download:
        print("  Note : Stage 1 skipped (--skip-download)")
    print(f"  Running {len(stages)} stage(s)")
    print(f"{'=' * WIDTH}")

    t_pipeline = time.monotonic()
    completed: list[tuple[str, float]] = []

    # Run each stage in sequence; run_stage exits immediately on failure
    for i, (label, cmd) in enumerate(stages, start=1):
        elapsed = run_stage(i, len(stages), label, cmd, args.dry_run)
        completed.append((label, elapsed))

    total_elapsed = time.monotonic() - t_pipeline

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"\n{'=' * WIDTH}")
    print("  PIPELINE COMPLETE — all stages succeeded")
    print(f"{'─' * WIDTH}")
    if args.skip_download:
        print(f"  [SKIPPED]  —  Download & aggregate TLC data")
    for i, (label, elapsed) in enumerate(completed, start=1):
        if args.dry_run:
            print(f"  [DRY RUN]  {i}.  {label}")
        else:
            print(f"  [OK]       {i}.  {label}  ({_fmt(elapsed)})")
    print(f"{'─' * WIDTH}")
    if not args.dry_run:
        print(f"  Total runtime : {_fmt(total_elapsed)}")
    print(f"{'=' * WIDTH}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
