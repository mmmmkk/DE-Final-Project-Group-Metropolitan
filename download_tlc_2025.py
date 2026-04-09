#!/usr/bin/env python3
"""Download and optionally aggregate NYC TLC monthly trip parquet files for a target year.

Examples
--------
python download_tlc_2025.py --year 2025 --modes yellow green
python download_tlc_2025.py --year 2025 --modes yellow --aggregate
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
VALID_MODES = ["yellow", "green", "fhv", "hvfhv"]


def month_strings() -> list[str]:
    return [f"{m:02d}" for m in range(1, 13)]


def build_url(mode: str, year: int, month: str) -> str:
    return f"{BASE_URL}/{mode}_tripdata_{year}-{month}.parquet"


def download_file(url: str, dest: Path, timeout: int = 60) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as r:
        if r.status_code != 200:
            print(f"[WARN] {r.status_code} for {url}")
            return False
        total = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
        print(f"[OK] Downloaded {dest} ({total / 1e6:.1f} MB)")
        return True


def normalize_mode_columns(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Keep a lightweight, mode-agnostic subset for downstream zone-date aggregation."""
    rename_map = {}
    # Timestamp harmonization
    for col in df.columns:
        lc = col.lower()
        if lc in {"tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"}:
            rename_map[col] = "pickup_datetime"
        elif lc in {"tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"}:
            rename_map[col] = "dropoff_datetime"
        elif lc in {"pulocationid", "pickup_location_id"}:
            rename_map[col] = "pickup_location_id"
        elif lc in {"dolocationid", "dropoff_location_id"}:
            rename_map[col] = "dropoff_location_id"
        elif lc == "trip_miles":
            rename_map[col] = "trip_distance"

    df = df.rename(columns=rename_map)

    keep_candidates = [
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "passenger_count",
        "trip_time",
        "base_passenger_fare",
        "tolls",
        "tips",
        "driver_pay",
        "congestion_surcharge",
        "cbd_congestion_fee",
    ]
    keep = [c for c in keep_candidates if c in df.columns]
    out = df[keep].copy()
    out["trip_mode"] = mode

    if "pickup_datetime" in out.columns:
        out["pickup_datetime"] = pd.to_datetime(out["pickup_datetime"], errors="coerce")
        out["date"] = out["pickup_datetime"].dt.date
    elif "dropoff_datetime" in out.columns:
        out["dropoff_datetime"] = pd.to_datetime(out["dropoff_datetime"], errors="coerce")
        out["date"] = out["dropoff_datetime"].dt.date
    else:
        out["date"] = pd.NaT

    return out


def aggregate_zone_date(df: pd.DataFrame) -> pd.DataFrame:
    # Base grouping
    base = df.dropna(subset=["date", "pickup_location_id"])

    # Trip counts
    counts = (
        base.groupby(["date", "pickup_location_id", "trip_mode"])
        .size()
        .reset_index(name="trips_pickup")
    )

    # Optional metrics
    agg_cols = {}
    if "trip_distance" in df.columns:
        agg_cols["trip_distance"] = "mean"
    if "fare_amount" in df.columns:
        agg_cols["fare_amount"] = "mean"
    if "total_amount" in df.columns:
        agg_cols["total_amount"] = "mean"

    if agg_cols:
        metrics = (
            base.groupby(["date", "pickup_location_id", "trip_mode"])
            .agg(agg_cols)
            .reset_index()
        )

        # merge counts + metrics
        grouped = counts.merge(
            metrics,
            on=["date", "pickup_location_id", "trip_mode"],
            how="left"
        )
    else:
        grouped = counts

    # final rename
    grouped = grouped.rename(columns={
        "pickup_location_id": "taxi_zone_id"
    })

    return grouped


def aggregate_all(files: Iterable[Path], mode: str, output_file: Path) -> None:
    frames = []
    for path in sorted(files):
        print(f"[INFO] Reading {path.name}")
        df = pd.read_parquet(path)
        frames.append(normalize_mode_columns(df, mode))
    if not frames:
        print(f"[WARN] No files found to aggregate for mode={mode}")
        return
    combined = pd.concat(frames, ignore_index=True)
    agg = aggregate_zone_date(combined)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(output_file, index=False)
    print(f"[OK] Wrote {output_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--modes", nargs="+", default=["yellow"], choices=VALID_MODES)
    parser.add_argument("--raw-dir", default="data/raw/tlc")
    parser.add_argument("--processed-dir", default="data/processed/tlc")
    parser.add_argument("--aggregate", action="store_true", help="Aggregate monthly files into zone-date parquet.")
    parser.add_argument("--skip-download", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)

    for mode in args.modes:
        monthly_files = []
        for month in month_strings():
            dest = raw_dir / f"{mode}_tripdata_{args.year}-{month}.parquet"
            monthly_files.append(dest)
            if not args.skip_download:
                url = build_url(mode, args.year, month)
                download_file(url, dest)

        if args.aggregate:
            output_file = processed_dir / f"{mode}_zone_date_{args.year}.parquet"
            aggregate_all(monthly_files, mode, output_file)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
