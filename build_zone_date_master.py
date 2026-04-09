#!/usr/bin/env python3
"""Merge TLC zone-date facts with static POI zone features and daily weather.

Example
-------
python build_zone_date_master.py \
  --tlc-files data/processed/tlc/yellow_zone_date_2025.parquet \
  --poi-file data/processed/poi/poi_zone_features.parquet \
  --weather-file data/processed/weather/weather_2025.csv \
  --output-file data/processed/final/zone_date_master_2025.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tlc-files", nargs="+", required=True)
    parser.add_argument("--poi-file", required=True)
    parser.add_argument("--weather-file", required=True)
    parser.add_argument("--output-file", required=True)
    return parser.parse_args()


def read_table(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.suffix.lower() == ".parquet":
        return pd.read_parquet(p)
    return pd.read_csv(p)


def main() -> int:
    args = parse_args()

    tlc_frames = []
    for file in args.tlc_files:
        df = read_table(file)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        if "taxi_zone_id" in df.columns:
            df["taxi_zone_id"] = pd.to_numeric(df["taxi_zone_id"], errors="coerce").astype("Int64")
        tlc_frames.append(df)

    tlc = pd.concat(tlc_frames, ignore_index=True)

    # If multiple modes are stacked long, pivot to wide on trip_mode if present.
    if "trip_mode" in tlc.columns:
        value_cols = [c for c in tlc.columns if c not in {"date", "taxi_zone_id", "trip_mode"}]
        tlc = tlc.pivot_table(index=["date", "taxi_zone_id"], columns="trip_mode", values=value_cols, aggfunc="first")
        tlc.columns = [f"{metric}_{mode}" for metric, mode in tlc.columns]
        tlc = tlc.reset_index()

    poi = read_table(args.poi_file)
    if "taxi_zone_id" in poi.columns:
        poi["taxi_zone_id"] = pd.to_numeric(poi["taxi_zone_id"], errors="coerce").astype("Int64")

    weather = read_table(args.weather_file)
    if "date" in weather.columns:
        weather["date"] = pd.to_datetime(weather["date"]).dt.date

    out = tlc.merge(poi, on="taxi_zone_id", how="left").merge(weather, on="date", how="left")

    output_file = Path(args.output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.suffix.lower() == ".csv":
        out.to_csv(output_file, index=False)
    else:
        out.to_parquet(output_file, index=False)
    print(f"[OK] Wrote {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
