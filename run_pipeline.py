#!/usr/bin/env python3
"""Orchestrate the full NYC TLC + POI + Weather data engineering pipeline.

Runs all stages from scratch for one or more years. POI features are built
once (they are not year-specific). TLC download, cleaning, weather, and the
master table run once per year. The DuckDB database is built once at the end
combining all years.

Usage
-----
python run_pipeline.py                        # run for 2025 only
python run_pipeline.py --years 2019 2020 2025 # run for all three years
python run_pipeline.py --dry-run              # print commands without executing
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

_SCRIPT_DIR = Path(__file__).parent


def _find_python() -> str:
    """Return a working Python interpreter path.

    sys.executable can point to a broken path when a venv was created on a
    different machine (e.g. a Mac Anaconda venv copied to Windows). To work
    around this, we check for the project venv first, then fall back to
    sys.executable, and finally to whatever 'python' is on PATH.
    """
    for rel in (".venv/Scripts/python.exe", ".venv/bin/python"):
        candidate = _SCRIPT_DIR / rel
        if candidate.exists():
            return str(candidate)
    if Path(sys.executable).exists():
        return sys.executable
    for name in ("python", "python3"):
        found = shutil.which(name)
        if found:
            return found
    raise RuntimeError(
        f"Cannot find a working Python interpreter (sys.executable={sys.executable!r}). "
        "Activate your virtual environment and re-run."
    )


# ── Year-independent paths ─────────────────────────────────────────────────────
RAW_TLC_DIR       = "data/raw/tlc"
PROCESSED_TLC_DIR = "data/processed/tlc"
POI_CSV           = "data/raw/poi/CommonPlace_20260408.csv"
ZONES_FILE        = "data/raw/taxi_zones/taxi_zones.shp"
POI_OUTPUT        = "data/processed/poi/poi_zone_features.parquet"
DB_PATH           = "db/nyc.duckdb"

WIDTH = 64


def tlc_output(year: int) -> str:
    return f"data/processed/tlc/yellow_zone_date_{year}.parquet"


def weather_output(year: int) -> str:
    return f"data/processed/weather/weather_{year}.csv"


def master_output(year: int) -> str:
    return f"data/processed/final/zone_date_master_{year}.parquet"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the full NYC data engineering pipeline.")
    p.add_argument(
        "--years", nargs="+", type=int, default=[2025],
        help="One or more years to process (e.g. --years 2019 2020 2025).",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print each stage command without executing it.",
    )
    return p.parse_args()


def _fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


def run_stage(n: int, total: int, label: str, cmd: list[str], dry_run: bool) -> float:
    """Run one pipeline stage as a subprocess.

    Prints the stage header, runs the command, and returns elapsed seconds.
    Exits immediately on non-zero return code so the pipeline stops.
    """
    bar = "-" * WIDTH
    print(f"\n{bar}")
    print(f"  Stage {n}/{total}: {label}")
    print(f"  Command : {' '.join(cmd)}")
    print(f"  Started : {datetime.now().strftime('%H:%M:%S')}")
    print(bar)

    if dry_run:
        print("  [DRY RUN] Skipping execution.")
        return 0.0

    t0 = time.monotonic()
    result = subprocess.run(cmd)
    elapsed = time.monotonic() - t0

    if result.returncode != 0:
        print(f"\n{'=' * WIDTH}")
        print(f"  PIPELINE FAILED")
        print(f"  Failed stage : {n}/{total} - {label}")
        print(f"  Exit code    : {result.returncode}")
        print(f"  Check the output above for details.")
        print(f"{'=' * WIDTH}")
        sys.exit(result.returncode)

    print(f"  Finished: {datetime.now().strftime('%H:%M:%S')}  ({_fmt(elapsed)})")
    return elapsed


def build_poi_stage() -> tuple[str, list[str]]:
    """POI features are zone-based and not year-specific — run once."""
    python = _find_python()
    return (
        "Build POI zone features",
        [
            python, "build_poi_zone_features.py",
            "--poi-csv",       POI_CSV,
            "--zones-file",    ZONES_FILE,
            "--output-file",   POI_OUTPUT,
            "--point-col",     "the_geom",
            "--category-cols", "FACILITY DOMAINS", "FACILITY TYPE",
        ],
    )


def build_year_stages(year: int) -> list[tuple[str, list[str]]]:
    """Return the 4 year-specific stages: download, clean, weather, master table."""
    python = _find_python()
    return [
        (
            f"[{year}] Download & aggregate TLC yellow taxi data",
            [
                python, "download_tlc_2025.py",
                "--year", str(year),
                "--modes", "yellow",
                "--raw-dir", RAW_TLC_DIR,
                "--processed-dir", PROCESSED_TLC_DIR,
                "--aggregate",
            ],
        ),
        (
            f"[{year}] Clean & re-aggregate TLC data",
            [
                python, "clean_tlc_2025.py",
                "--year",          str(year),
                "--modes",         "yellow",
                "--raw-dir",       RAW_TLC_DIR,
                "--processed-dir", PROCESSED_TLC_DIR,
            ],
        ),
        (
            f"[{year}] Fetch NOAA daily weather",
            [
                python, "build_weather_2025.py",
                "--start-date",  f"{year}-01-01",
                "--end-date",    f"{year}-12-31",
                "--output-file", weather_output(year),
            ],
        ),
        (
            f"[{year}] Build zone x date master table",
            [
                python, "build_zone_date_master.py",
                "--tlc-files",    tlc_output(year),
                "--poi-file",     POI_OUTPUT,
                "--weather-file", weather_output(year),
                "--output-file",  master_output(year),
            ],
        ),
    ]


def build_db_stage(tlc_files: list[str], weather_files: list[str]) -> tuple[str, list[str]]:
    """DB stage runs once after all years, combining all year outputs."""
    python = _find_python()
    return (
        "Build DuckDB database",
        [
            python, "build_database.py",
            "--db-path",       DB_PATH,
            "--tlc-files",     *tlc_files,
            "--poi-file",      POI_OUTPUT,
            "--weather-files", *weather_files,
        ],
    )


def main() -> int:
    args = parse_args()
    years = sorted(args.years)

    # Build the full ordered stage list:
    #   1 POI stage + (4 stages x N years) + 1 DB stage
    all_stages: list[tuple[str, list[str]]] = [build_poi_stage()]
    for year in years:
        all_stages.extend(build_year_stages(year))
    all_stages.append(build_db_stage(
        [tlc_output(y) for y in years],
        [weather_output(y) for y in years],
    ))

    total = len(all_stages)

    print(f"\n{'=' * WIDTH}")
    print("  NYC Data Engineering Pipeline")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.dry_run:
        print("  Mode  : DRY RUN (no commands will execute)")
    print(f"  Years : {', '.join(str(y) for y in years)}")
    print(f"  Running {total} stage(s)")
    print(f"{'=' * WIDTH}")

    t_pipeline = time.monotonic()
    completed: list[tuple[str, float]] = []

    for i, (label, cmd) in enumerate(all_stages, start=1):
        elapsed = run_stage(i, total, label, cmd, args.dry_run)
        completed.append((label, elapsed))

    total_elapsed = time.monotonic() - t_pipeline

    print(f"\n{'=' * WIDTH}")
    print("  PIPELINE COMPLETE - all stages succeeded")
    print(f"{'-' * WIDTH}")
    for i, (label, elapsed) in enumerate(completed, start=1):
        if args.dry_run:
            print(f"  [DRY RUN]  {i}.  {label}")
        else:
            print(f"  [OK]       {i}.  {label}  ({_fmt(elapsed)})")
    print(f"{'-' * WIDTH}")
    if not args.dry_run:
        print(f"  Total runtime : {_fmt(total_elapsed)}")
    print(f"{'=' * WIDTH}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
