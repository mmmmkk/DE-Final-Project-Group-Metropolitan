#!/usr/bin/env python3
"""Aggregate NYC DOHMH restaurant inspection data to taxi-zone level features
and merge them into the zone×date master table.

Pipeline
--------
1. Load restaurant inspection CSV and drop rows with no valid coordinates.
2. Spatially join each restaurant to a taxi zone (point-in-polygon).
3. Aggregate static zone-level restaurant features:
     - total restaurant count
     - count by cuisine category (top-level grouping)
     - share of restaurants with grade A
     - median inspection score (lower = better)
     - critical violation rate
4. Merge the zone features into the existing master parquet on taxi_zone_id.

Example
-------
python build_restaurant_zone_features.py \
  --restaurant-csv  data/raw/Restaurant/DOHMH_New_York_City_Restaurant_Inspection_Results_20260414.csv \
  --zones-file      data/raw/taxi_zones/taxi_zones.shp \
  --master-file     data/processed/final/zone_date_master_2025.parquet \
  --output-file     data/processed/final/zone_date_master_2025.parquet

To write to a separate file instead of overwriting the master, pass a different --output-file.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd


# ---------------------------------------------------------------------------
# Cuisine grouping
# ---------------------------------------------------------------------------

CUISINE_GROUPS: dict[str, list[str]] = {
    "american":    ["american", "hamburgers", "hotdogs", "sandwiches", "steak", "barbecue"],
    "asian":       ["chinese", "japanese", "korean", "thai", "vietnamese", "asian", "sushi",
                    "bangladeshi", "filipino", "indonesian", "malaysian", "taiwanese"],
    "italian":     ["italian", "pizza"],
    "latin":       ["mexican", "latin", "spanish", "peruvian", "colombian", "cuban",
                    "caribbean", "dominican", "salvadoran"],
    "middle_east": ["middle eastern", "turkish", "greek", "mediterranean", "moroccan",
                    "afghan", "pakistani", "halal"],
    "south_asian": ["indian", "bangladeshi"],
    "african":     ["african", "ethiopian"],
    "bakery_cafe": ["bakery", "café", "cafe", "coffee", "juice", "smoothie", "ice cream",
                    "donuts", "pancakes", "waffles"],
    "seafood":     ["seafood", "fish"],
    "vegetarian":  ["vegetarian", "vegan"],
}


def map_cuisine(raw: str) -> str:
    if pd.isna(raw):
        return "other"
    lower = raw.lower()
    for group, keywords in CUISINE_GROUPS.items():
        if any(k in lower for k in keywords):
            return group
    return "other"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_zones(zones_file: str) -> gpd.GeoDataFrame:
    zones = gpd.read_file(zones_file)
    if "LocationID" not in zones.columns:
        candidates = [c for c in zones.columns if c.lower() == "locationid"]
        if candidates:
            zones = zones.rename(columns={candidates[0]: "LocationID"})
        else:
            raise ValueError("Taxi zone file must contain a LocationID column.")
    return zones[["LocationID", "zone", "borough", "geometry"]].copy()


def load_restaurants(csv_path: str) -> gpd.GeoDataFrame:
    df = pd.read_csv(csv_path)

    # Keep one representative row per restaurant (unique CAMIS).
    # Use the most recent non-placeholder inspection date so grade/score reflect
    # the latest known state of each establishment.
    df["INSPECTION DATE"] = pd.to_datetime(df["INSPECTION DATE"], format="%m/%d/%Y", errors="coerce")
    placeholder = pd.Timestamp("1900-01-01")
    df = df[df["INSPECTION DATE"] != placeholder]
    df = df.sort_values("INSPECTION DATE", ascending=False)
    df = df.drop_duplicates(subset=["CAMIS"], keep="first")

    # Coordinates — drop invalid ones
    df["Latitude"]  = pd.to_numeric(df["Latitude"],  errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df = df.dropna(subset=["Latitude", "Longitude"])
    df = df[(df["Latitude"] != 0) & (df["Longitude"] != 0)]

    # Derived fields used in aggregation
    df["grade_a"]        = (df["GRADE"].str.strip() == "A").astype(int)
    df["score"]          = pd.to_numeric(df["SCORE"], errors="coerce")
    df["critical_flag"]  = (df["CRITICAL FLAG"].str.strip() == "Critical").astype(int)
    df["cuisine_group"]  = df["CUISINE DESCRIPTION"].map(map_cuisine)

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Longitude"], df["Latitude"]),
        crs="EPSG:4326",
    )
    return gdf


def build_zone_features(joined: gpd.GeoDataFrame) -> pd.DataFrame:
    """Aggregate restaurant data to one row per taxi_zone_id."""

    # --- base stats ---
    base = (
        joined.groupby("taxi_zone_id")
        .agg(
            restaurant_count      = ("CAMIS",         "size"),
            restaurant_grade_a_pct= ("grade_a",        "mean"),
            restaurant_score_median=("score",          "median"),
            restaurant_critical_pct=("critical_flag",  "mean"),
        )
        .reset_index()
    )
    base["restaurant_grade_a_pct"]  = base["restaurant_grade_a_pct"].round(4)
    base["restaurant_score_median"] = base["restaurant_score_median"].round(2)
    base["restaurant_critical_pct"] = base["restaurant_critical_pct"].round(4)

    # --- cuisine breakdown ---
    cuisine_counts = (
        joined.pivot_table(
            index="taxi_zone_id",
            columns="cuisine_group",
            values="CAMIS",
            aggfunc="size",
            fill_value=0,
        )
        .reset_index()
    )
    cuisine_counts.columns = [
        "taxi_zone_id" if c == "taxi_zone_id" else f"restaurant_count_{c}"
        for c in cuisine_counts.columns
    ]

    out = base.merge(cuisine_counts, on="taxi_zone_id", how="left")
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--restaurant-csv", required=True,
                        help="Path to DOHMH restaurant inspection CSV.")
    parser.add_argument("--zones-file", required=True,
                        help="Taxi zone polygon file (.shp / .geojson / .gpkg).")
    parser.add_argument("--master-file", required=True,
                        help="Existing zone×date master parquet to merge into.")
    parser.add_argument("--output-file", required=True,
                        help="Where to write the enriched master parquet.")
    parser.add_argument("--restaurant-features-file", default=None,
                        help="Optional path to also save the raw zone-level restaurant "
                             "features parquet (for inspection / reuse).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print("[1/4] Loading taxi zones ...")
    zones = load_zones(args.zones_file)

    print("[2/4] Loading and preparing restaurant data ...")
    restaurants = load_restaurants(args.restaurant_csv)
    print(f"      {len(restaurants):,} unique restaurants with valid coordinates")

    # Reproject restaurants to match zone CRS before spatial join
    restaurants = restaurants.to_crs(zones.crs)

    print("[3/4] Spatial join: assigning restaurants to taxi zones ...")
    joined = gpd.sjoin(restaurants, zones, how="left", predicate="within")
    joined = joined.rename(columns={"LocationID": "taxi_zone_id"})
    unmatched = joined["taxi_zone_id"].isna().sum()
    if unmatched:
        print(f"      Warning: {unmatched:,} restaurants outside any taxi zone — excluded from aggregation")
    joined = joined.dropna(subset=["taxi_zone_id"])
    joined["taxi_zone_id"] = joined["taxi_zone_id"].astype("Int64")

    zone_features = build_zone_features(joined)
    print(f"      Zone features built for {len(zone_features):,} taxi zones")

    if args.restaurant_features_file:
        feat_path = Path(args.restaurant_features_file)
        feat_path.parent.mkdir(parents=True, exist_ok=True)
        zone_features.to_parquet(feat_path, index=False)
        print(f"      [OK] Zone features saved to {feat_path}")

    print("[4/4] Merging restaurant features into master table ...")
    master = pd.read_parquet(args.master_file)
    master["taxi_zone_id"] = pd.to_numeric(master["taxi_zone_id"], errors="coerce").astype("Int64")

    # Drop any stale restaurant columns from a previous run before re-merging
    restaurant_cols = [c for c in master.columns if c.startswith("restaurant_")]
    if restaurant_cols:
        master = master.drop(columns=restaurant_cols)
        print(f"      Dropped {len(restaurant_cols)} stale restaurant columns before re-merge")

    enriched = master.merge(zone_features, on="taxi_zone_id", how="left")

    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_parquet(output_path, index=False)
    print(f"[OK] Wrote {output_path}  ({enriched.shape[0]:,} rows × {enriched.shape[1]} columns)")

    # Quick coverage report
    matched = enriched["restaurant_count"].notna().sum()
    total   = len(enriched)
    print(f"     Restaurant features matched for {matched:,}/{total:,} master rows "
          f"({100*matched/total:.1f}%)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
