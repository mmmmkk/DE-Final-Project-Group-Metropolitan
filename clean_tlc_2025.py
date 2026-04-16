#!/usr/bin/env python3
"""Clean and aggregate NYC TLC monthly trip parquet files for a target year.

This script is intended to run after raw monthly parquet files have already been
downloaded into data/raw/tlc/.

Example
-------
python clean_tlc_2025.py --year 2025 --modes yellow --raw-dir data/raw/tlc --processed-dir data/processed/tlc
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

VALID_MODES = ["yellow", "green", "fhv", "hvfhv"]


def month_strings() -> list[str]:
    return [f"{m:02d}" for m in range(1, 13)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--modes", nargs="+", default=["yellow"], choices=VALID_MODES)
    parser.add_argument("--raw-dir", default="data/raw/tlc")
    parser.add_argument("--processed-dir", default="data/processed/tlc")
    return parser.parse_args()


def normalize_mode_columns(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Keep a lightweight, mode-agnostic subset for downstream zone-date aggregation."""
    rename_map = {}

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


def clean_tlc_records(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Task-aware cleaning for 2025 zone-date analysis."""
    out = df.copy()

    n_before = len(out)

    # Parse / standardize date
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out = out.dropna(subset=["date"])

    # Keep only target year
    out = out[pd.to_datetime(out["date"]).dt.year == year]

    # Standardize zone IDs
    if "pickup_location_id" in out.columns:
        out["pickup_location_id"] = pd.to_numeric(out["pickup_location_id"], errors="coerce")
        out = out[out["pickup_location_id"].between(1, 263, inclusive="both")]

    if "dropoff_location_id" in out.columns:
        out["dropoff_location_id"] = pd.to_numeric(out["dropoff_location_id"], errors="coerce")
        out.loc[~out["dropoff_location_id"].between(1, 263, inclusive="both"), "dropoff_location_id"] = pd.NA

    # Standardize numeric columns
    numeric_cols = [
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
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # Rule-based cleaning: allow nulls, but drop obviously invalid negatives
    nonnegative_cols = [
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
    for col in nonnegative_cols:
        if col in out.columns:
            out = out[(out[col].isna()) | (out[col] >= 0)]

    # Optional consistency rule
    if "pickup_datetime" in out.columns and "dropoff_datetime" in out.columns:
        out["pickup_datetime"] = pd.to_datetime(out["pickup_datetime"], errors="coerce")
        out["dropoff_datetime"] = pd.to_datetime(out["dropoff_datetime"], errors="coerce")
        valid_duration = (
            out["pickup_datetime"].isna()
            | out["dropoff_datetime"].isna()
            | (out["dropoff_datetime"] >= out["pickup_datetime"])
        )
        out = out[valid_duration]

    print(f"[INFO] Rows before cleaning: {n_before:,}")
    print(f"[INFO] Rows after cleaning:  {len(out):,}")
    if len(out) > 0:
        print(f"[INFO] Date range after cleaning: {out['date'].min()} -> {out['date'].max()}")

    return out


def aggregate_zone_date(df: pd.DataFrame) -> pd.DataFrame:
    base = df.dropna(subset=["date", "pickup_location_id"])
    group_cols = ["date", "pickup_location_id", "trip_mode"]

    counts = (
        base.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name="trips_pickup")
    )

    agg_cols = {}
    if "trip_distance" in df.columns:
        agg_cols["trip_distance"] = "mean"
    if "fare_amount" in df.columns:
        agg_cols["fare_amount"] = "mean"
    if "total_amount" in df.columns:
        agg_cols["total_amount"] = "mean"

    if agg_cols:
        metrics = (
            base.groupby(group_cols, dropna=False)
            .agg(agg_cols)
            .reset_index()
        )
        grouped = counts.merge(metrics, on=group_cols, how="left")
    else:
        grouped = counts

    grouped = grouped.rename(columns={"pickup_location_id": "taxi_zone_id"})
    return grouped


def aggregate_all(files: Iterable[Path], mode: str, year: int, output_file: Path) -> None:
    frames = []

    for path in sorted(files):
        if not path.exists():
            print(f"[WARN] Missing file: {path}")
            continue
        print(f"[INFO] Reading {path.name}")
        df = pd.read_parquet(path)
        frames.append(normalize_mode_columns(df, mode))

    if not frames:
        print(f"[WARN] No files found to process for mode={mode}")
        return

    combined = pd.concat(frames, ignore_index=True)
    cleaned = clean_tlc_records(combined, year)
    agg = aggregate_zone_date(cleaned)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(output_file, index=False)
    print(f"[OK] Wrote {output_file}")


def main() -> int:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)

    for mode in args.modes:
        monthly_files = [
            raw_dir / f"{mode}_tripdata_{args.year}-{month}.parquet"
            for month in month_strings()
        ]
        output_file = processed_dir / f"{mode}_zone_date_{args.year}.parquet"
        aggregate_all(monthly_files, mode, args.year, output_file)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())