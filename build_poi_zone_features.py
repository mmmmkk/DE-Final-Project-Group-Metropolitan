#!/usr/bin/env python3
"""Clean NYC CommonPlace/POI data and aggregate static zone-level features.

Expected inputs
---------------
1) CommonPlace/POI CSV export from NYC Open Data
2) Taxi zone polygons (shapefile, GeoJSON, or GeoPackage)

Example
-------
python build_poi_zone_features.py \
  --poi-csv data/raw/poi/commonplace_2026-04-04.csv \
  --zones-file data/raw/taxi_zones/taxi_zones.shp \
  --output-file data/processed/poi/poi_zone_features.parquet
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import wkt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--poi-csv", required=True)
    parser.add_argument("--zones-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--poi-name-col", default="FEATURE NAME")
    parser.add_argument("--point-col", default=None, help="Optional point-geometry column if lat/lon columns are absent.")
    parser.add_argument("--lat-col", default=None)
    parser.add_argument("--lon-col", default=None)
    parser.add_argument("--category-cols", nargs="*", default=["FACI_DOM", "FACILITY_T", "THEME"])
    return parser.parse_args()


def normalize_text(s: object) -> str:
    s = "" if pd.isna(s) else str(s).strip().lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def parse_point_column(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    # Handles common Socrata/WKT patterns like POINT (-73.98 40.75)
    extracted = series.astype(str).str.extract(r"POINT\s*\(([-0-9.]+)\s+([-0-9.]+)\)")
    lon = pd.to_numeric(extracted[0], errors="coerce")
    lat = pd.to_numeric(extracted[1], errors="coerce")
    return lat, lon


def infer_category(row: pd.Series, category_cols: list[str]) -> str:
    values = " | ".join([str(row[c]) for c in category_cols if c in row and pd.notna(row[c])]).lower()
    if any(k in values for k in ["museum", "theater", "library", "culture", "historic", "landmark"]):
        return "culture_attraction"
    if any(k in values for k in ["park", "garden", "recreation", "playground", "beach"]):
        return "park_recreation"
    if any(k in values for k in ["station", "terminal", "airport", "ferry", "transit", "transport"]):
        return "transport_hub"
    if any(k in values for k in ["school", "college", "education", "university"]):
        return "education"
    if any(k in values for k in ["hospital", "clinic", "health"]):
        return "health"
    if any(k in values for k in ["government", "civic", "court", "police", "fire"]):
        return "civic_public_service"
    return "other"


def load_zones(zones_file: str) -> gpd.GeoDataFrame:
    zones = gpd.read_file(zones_file)
    # Common TLC fields include LocationID, zone, borough
    if "LocationID" not in zones.columns:
        candidates = [c for c in zones.columns if c.lower() == "locationid"]
        if candidates:
            zones = zones.rename(columns={candidates[0]: "LocationID"})
        else:
            raise ValueError("Taxi zone file must contain a LocationID column.")
    return zones[[c for c in ["LocationID", "zone", "borough", "geometry"] if c in zones.columns]].copy()


def load_poi(args: argparse.Namespace) -> gpd.GeoDataFrame:
    poi = pd.read_csv(args.poi_csv)
    if args.lat_col and args.lon_col and args.lat_col in poi.columns and args.lon_col in poi.columns:
        lat = pd.to_numeric(poi[args.lat_col], errors="coerce")
        lon = pd.to_numeric(poi[args.lon_col], errors="coerce")
    elif args.point_col and args.point_col in poi.columns:
        lat, lon = parse_point_column(poi[args.point_col])
    else:
        # Try common names automatically
        common_lat = next((c for c in poi.columns if c.lower() in {"latitude", "lat"}), None)
        common_lon = next((c for c in poi.columns if c.lower() in {"longitude", "lon", "lng"}), None)
        if common_lat and common_lon:
            lat = pd.to_numeric(poi[common_lat], errors="coerce")
            lon = pd.to_numeric(poi[common_lon], errors="coerce")
        else:
            point_candidate = next((c for c in poi.columns if "point" in c.lower() or "location" == c.lower()), None)
            if point_candidate is None:
                raise ValueError("Could not infer coordinate columns. Pass --lat-col/--lon-col or --point-col.")
            lat, lon = parse_point_column(poi[point_candidate])

    poi = poi.copy()
    poi["latitude"] = lat
    poi["longitude"] = lon
    poi = poi.dropna(subset=["latitude", "longitude"])
    poi = poi[
    poi["latitude"].between(40.4, 41.0, inclusive="both") &
    poi["longitude"].between(-74.3, -73.6, inclusive="both")
    ]
    poi["feature_name"] = poi[args.poi_name_col].astype(str).str.strip() if args.poi_name_col in poi.columns else ""
    poi["feature_name_norm"] = poi["feature_name"].map(normalize_text)
    poi["lon_round"] = poi["longitude"].round(6)
    poi["lat_round"] = poi["latitude"].round(6)
    poi["poi_category"] = poi.apply(lambda row: infer_category(row, args.category_cols), axis=1)
    poi = poi.drop_duplicates(subset=["feature_name_norm", "lon_round", "lat_round"])

    gdf = gpd.GeoDataFrame(
        poi,
        geometry=gpd.points_from_xy(poi["longitude"], poi["latitude"]),
        crs="EPSG:4326",
    )
    return gdf


def main() -> int:
    args = parse_args()
    zones = load_zones(args.zones_file)
    poi = load_poi(args)

    if zones.crs is None:
        zones = zones.set_crs("EPSG:4326")
    poi = poi.to_crs(zones.crs)

    joined = gpd.sjoin(poi, zones, how="left", predicate="within")
    joined = joined.rename(columns={"LocationID": "taxi_zone_id"})
    joined = joined.dropna(subset=["taxi_zone_id"])
    print("poi rows before spatial join:", len(poi))
    print("poi rows after spatial join:", len(joined))
    print("matched taxi zones:", joined["taxi_zone_id"].notna().sum())

    zone_counts = (
        joined.groupby("taxi_zone_id")
        .agg(
            poi_count_total=("feature_name", "size"),
            poi_name_examples=("feature_name", lambda s: "; ".join(sorted(pd.unique(s.dropna()))[:10])),
        )
        .reset_index()
    )

    cat_counts = (
        joined.pivot_table(index="taxi_zone_id", columns="poi_category", values="feature_name", aggfunc="size", fill_value=0)
        .reset_index()
    )
    cat_counts.columns = [str(c) if c == "taxi_zone_id" else f"poi_count_{c}" for c in cat_counts.columns]

    out = zone_counts.merge(cat_counts, on="taxi_zone_id", how="left")
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
