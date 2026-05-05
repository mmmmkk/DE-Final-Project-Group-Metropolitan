#!/usr/bin/env python3
"""
MASTER PIPELINE ORCHESTRATOR

This script coordinates the entire NYC Data Engineering pipeline end-to-end.

It executes multiple modular scripts in sequence and ensures:
- Reproducibility
- Clear stage-wise execution
- Immediate failure on errors (fail-fast)

PIPELINE FLOW:
--------------
1. Ensure taxi zones shapefile (auto-download)
2. Build POI features (once)
3. For each year:
   - Download TLC data
   - Clean TLC data
   - Fetch weather
   - Build master dataset
4. Build DuckDB database

IMPORTANT:
----------
POI dataset MUST exist manually at:
data/raw/poi/CommonPlace_20260408.csv
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

import os
import zipfile
import requests


def ensure_taxi_zones():
    """
    Ensures taxi zones shapefile exists locally.

    This is REQUIRED for spatial joins between POI data and taxi zones.

    Behavior:
    - If exists → skip
    - If missing → download + extract

    Common issues:
    - Network failure → download incomplete
    - Nested zip extraction → shapefile not found
    """

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    output_dir = "data/raw/taxi_zones"
    shp_path = os.path.join(output_dir, "taxi_zones.shp")

    # Skip if already present
    if os.path.exists(shp_path):
        print("Taxi zones already exist. Skipping download.")
        return

    print("Downloading taxi zones shapefile...")

    os.makedirs(output_dir, exist_ok=True)
    zip_path = os.path.join(output_dir, "taxi_zones.zip")

    # Download ZIP
    r = requests.get(url, stream=True)
    r.raise_for_status()

    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    # Extract ZIP
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)

    # 🔧 FIX: handle nested folder issue
    nested_dir = os.path.join(output_dir, "taxi_zones")
    if os.path.exists(nested_dir):
        for file in os.listdir(nested_dir):
            os.replace(
                os.path.join(nested_dir, file),
                os.path.join(output_dir, file)
            )
        os.rmdir(nested_dir)

    # Remove ZIP after extraction
    os.remove(zip_path)

    # Validate shapefile exists
    if not os.path.exists(shp_path):
        raise RuntimeError("Taxi zones download failed: shapefile not found")

    print("Taxi zones ready.")


def _find_python() -> str:
    """
    Finds a working Python interpreter.

    Why:
    - Virtual environments may break across machines
    - sys.executable may be invalid

    Order:
    1. Local venv
    2. sys.executable
    3. system python

    Failure:
    - Recreate venv
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

    raise RuntimeError("No working Python interpreter found.")


# -------- PATH CONFIG --------

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
    """
    Parse CLI arguments.

    --years → years to process
    --dry-run → print commands only
    """
    p = argparse.ArgumentParser()

    p.add_argument("--years", nargs="+", type=int, default=[2025])
    p.add_argument("--dry-run", action="store_true")

    return p.parse_args()


def _fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


def run_stage(n, total, label, cmd, dry_run):
    """
    Executes ONE stage.

    - Prints metadata
    - Runs subprocess
    - Stops pipeline if failure occurs
    """

    bar = "-" * WIDTH
    print(f"\n{bar}")
    print(f"  Stage {n}/{total}: {label}")
    print(f"  Command : {' '.join(cmd)}")
    print(f"  Started : {datetime.now().strftime('%H:%M:%S')}")
    print(bar)

    if dry_run:
        print("DRY RUN")
        return 0

    t0 = time.monotonic()
    result = subprocess.run(cmd)
    elapsed = time.monotonic() - t0

    if result.returncode != 0:
        print("PIPELINE FAILED")
        sys.exit(result.returncode)

    print(f"Finished ({_fmt(elapsed)})")
    return elapsed


def build_poi_stage():
    """
    Build POI zone features.

    Runs ONCE because POI is not year-specific.

    Uses:
    - POI CSV (manual input)
    - Taxi zones shapefile (auto)

    Output:
    - zone-level POI features
    """
    python = _find_python()

    return (
        "Build POI zone features",
        [
            python, "build_poi_zone_features.py",
            "--poi-csv", POI_CSV,
            "--zones-file", ZONES_FILE,
            "--output-file", POI_OUTPUT,
            "--point-col", "the_geom",
            "--category-cols", "FACILITY DOMAINS", "FACILITY TYPE",
        ],
    )


def build_year_stages(year):
    """
    Returns 4 stages per year:

    1. Download TLC data
    2. Clean TLC data
    3. Fetch weather
    4. Build master dataset

    Each stage depends on previous outputs.
    """

    python = _find_python()

    return [
        (
            f"[{year}] Download TLC",
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
            f"[{year}] Clean TLC",
            [
                python, "clean_tlc_2025.py",
                "--year", str(year),
                "--modes", "yellow",
                "--raw-dir", RAW_TLC_DIR,
                "--processed-dir", PROCESSED_TLC_DIR,
            ],
        ),
        (
            f"[{year}] Weather",
            [
                python, "build_weather_2025.py",
                "--start-date", f"{year}-01-01",
                "--end-date", f"{year}-12-31",
                "--output-file", weather_output(year),
            ],
        ),
        (
            f"[{year}] Master dataset",
            [
                python, "build_zone_date_master.py",
                "--tlc-files", tlc_output(year),
                "--poi-file", POI_OUTPUT,
                "--weather-file", weather_output(year),
                "--output-file", master_output(year),
            ],
        ),
    ]


def build_db_stage(tlc_files, weather_files):
    """
    Final stage:
    Builds DuckDB database.

    Combines:
    - All TLC outputs
    - POI features
    - Weather data

    Output:
    - Analytics-ready DB
    """
    python = _find_python()

    return (
        "Build DuckDB database",
        [
            python, "build_database.py",
            "--db-path", DB_PATH,
            "--tlc-files", *tlc_files,
            "--poi-file", POI_OUTPUT,
            "--weather-files", *weather_files,
        ],
    )


def main():
    """
    Main pipeline execution.

    Steps:
    1. Ensure dependencies
    2. Build stage list
    3. Execute sequentially
    """

    ensure_taxi_zones()

    args = parse_args()
    years = sorted(args.years)

    # Build full pipeline
    all_stages = [build_poi_stage()]

    for year in years:
        all_stages.extend(build_year_stages(year))

    all_stages.append(
        build_db_stage(
            [tlc_output(y) for y in years],
            [weather_output(y) for y in years],
        )
    )

    total = len(all_stages)

    print("PIPELINE START")

    for i, (label, cmd) in enumerate(all_stages, start=1):
        run_stage(i, total, label, cmd, args.dry_run)

    print("PIPELINE COMPLETE")


if __name__ == "__main__":
    raise SystemExit(main())