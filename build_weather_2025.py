#!/usr/bin/env python3
"""Download and clean NOAA daily weather for NYC (Central Park by default).

Example
-------
python build_weather_2025.py \
  --station USW00094728 \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --output-file data/processed/weather/weather_2025.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://www.ncei.noaa.gov/access/services/data/v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="USW00094728")
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--end-date", default="2025-12-31")
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--units", default="metric", choices=["metric", "standard"])
    return parser.parse_args()


def fetch_weather(station: str, start_date: str, end_date: str, units: str) -> pd.DataFrame:
    params = {
        "dataset": "daily-summaries",
        "stations": station,
        "startDate": start_date,
        "endDate": end_date,
        "dataTypes": "TMAX,TMIN,PRCP,AWND,SNOW",
        "units": units,
        "includeAttributes": "false",
        "includeStationName": "false",
        "format": "json",
    }
    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data)


def clean_weather(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ["DATE", "TMAX", "TMIN", "PRCP", "AWND", "SNOW"]:
        if col in df.columns and col != "DATE":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    out = pd.DataFrame({"date": df["date"]})
    if "TMAX" in df.columns:
        out["tmax"] = df["TMAX"]
    if "TMIN" in df.columns:
        out["tmin"] = df["TMIN"]
    if "PRCP" in df.columns:
        out["prcp"] = df["PRCP"]
        out.loc[out["prcp"] < 0, "prcp"] = pd.NA # sanity check
    if "AWND" in df.columns:
        out["awnd"] = df["AWND"]
        out.loc[out["awnd"] < 0, "awnd"] = pd.NA
    if "SNOW" in df.columns:
        out["snow"] = df["SNOW"]
        out.loc[out["snow"] < 0, "snow"] = pd.NA

    if {"tmax", "tmin"}.issubset(out.columns):
        out.loc[out["tmax"] < out["tmin"], ["tmax", "tmin"]] = pd.NA
        out["temp_avg"] = (out["tmax"] + out["tmin"]) / 2
    if "prcp" in out.columns:
        out["rainy_day"] = out["prcp"].fillna(0).gt(0).astype(int)
        out["heavy_rain_day"] = out["prcp"].fillna(0).ge(10).astype(int)
    if "temp_avg" in out.columns:
        out["pleasant_temp_day"] = out["temp_avg"].between(15, 27, inclusive="both").astype(int)


    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    #out = out.sort_values("date").reset_index(drop=True)
    return out


def main() -> int:
    args = parse_args()
    raw = fetch_weather(args.station, args.start_date, args.end_date, args.units)
    weather = clean_weather(raw)
    output_file = Path(args.output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.suffix.lower() == ".parquet":
        weather.to_parquet(output_file, index=False)
    else:
        weather.to_csv(output_file, index=False)
    print(f"[OK] Wrote {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
