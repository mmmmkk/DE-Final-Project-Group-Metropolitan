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

# Source: https://github.com/CityOfNewYork/nyc-geo-metadata/blob/main/Metadata/Metadata_PointsOfInterest.md
FACILITY_TYPE_LABELS: dict[int, str] = {
    1:  "residential",
    2:  "education",
    3:  "cultural",
    4:  "recreational",
    5:  "social_services",
    6:  "transportation",
    7:  "commercial",
    8:  "government",
    9:  "religious",
    10: "health",
    11: "public_safety",
    12: "water",
    13: "miscellaneous",
}

FACILITY_DOMAIN_LABELS: dict[int, dict[int, str]] = {
    1: {1: "Gated Development", 2: "Private Development", 3: "Public Housing Development",
        4: "Constituent", 5: "Other"},
    2: {1: "Public Elementary School", 2: "Public Junior High/Middle", 3: "Public High School",
        4: "Private Elementary School", 5: "Private Junior/Middle School", 6: "Private High School",
        7: "Post Secondary Institution", 8: "Other", 9: "Public Early Childhood",
        10: "Public K-8", 11: "Public K-12", 12: "Public Secondary School",
        13: "Public School Building", 14: "Public School Annex",
        15: "Private Early Childhood", 16: "Private K-8",
        17: "Private K-12", 18: "Private Secondary School"},
    3: {1: "Center", 2: "Library", 3: "Theater/Concert Hall", 4: "Museum", 5: "Other"},
    4: {1: "Park", 2: "Amusement Park", 3: "Golf Course", 4: "Beach",
        5: "Botanical Garden", 6: "Zoo", 7: "Recreational Center",
        8: "Sports", 9: "Playground", 10: "Other", 11: "Pool", 12: "Garden"},
    5: {1: "Residential Child Care", 2: "Day Care Center", 3: "Adult Day Care",
        4: "Nursing Home/Assisted Living", 5: "Homeless Shelter", 6: "Other"},
    6: {1: "Bus Terminal", 2: "Ferry Terminal", 3: "Transit/Maintenance Yard",
        4: "Airport", 5: "Heliport", 6: "Marina", 7: "Pier",
        8: "Bridge", 9: "Tunnel", 10: "Exit/Entrance", 11: "Water Navigation", 12: "Other"},
    7: {1: "Center", 2: "Business", 3: "Market", 4: "Hotel/Motel", 5: "Restaurant", 6: "Other"},
    8: {1: "Government Office", 2: "Court of Law", 3: "Post Office",
        4: "Consulate", 5: "Embassy", 6: "Military", 7: "Other"},
    9: {1: "Church", 2: "Synagogue", 3: "Temple", 4: "Convent/Monastery", 5: "Mosque", 6: "Other"},
    10: {1: "Hospital", 2: "Inpatient Care Center", 3: "Outpatient Clinic", 4: "Other"},
    11: {1: "NYPD Precinct", 2: "NYPD Checkpoint", 3: "FDNY Ladder Company",
         4: "FDNY Battalion", 5: "Correctional Facility", 6: "FDNY Engine Company",
         7: "FDNY Special Unit", 8: "FDNY Division", 9: "FDNY Squad",
         10: "NYPD Other", 11: "Other", 12: "FDNY Other"},
    12: {1: "Island", 2: "River", 3: "Lake", 4: "Stream", 5: "Other", 6: "Pond"},
    13: {1: "Official Landmark", 2: "Point of Interest", 3: "Cemetery/Morgue", 4: "Other"},
}


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


def map_poi_category(facility_type: object, facility_domain: object) -> tuple[str, str]:
    """Return (poi_category, facility_domain_name) from numeric codes."""
    ft = int(facility_type) if pd.notna(facility_type) else None
    fd = int(facility_domain) if pd.notna(facility_domain) else None
    category = FACILITY_TYPE_LABELS.get(ft, "unknown") if ft is not None else "unknown"
    domain = FACILITY_DOMAIN_LABELS.get(ft, {}).get(fd, "Unknown") if ft is not None else "Unknown"
    return category, domain


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

    if "FACILITY TYPE" in poi.columns and "FACILITY DOMAINS" in poi.columns:
        ft_col = pd.to_numeric(poi["FACILITY TYPE"], errors="coerce")
        fd_col = pd.to_numeric(poi["FACILITY DOMAINS"], errors="coerce")
        mapped = [map_poi_category(ft, fd) for ft, fd in zip(ft_col, fd_col)]
        poi["poi_category"] = [m[0] for m in mapped]
        poi["facility_domain_name"] = [m[1] for m in mapped]
    else:
        poi["poi_category"] = "unknown"
        poi["facility_domain_name"] = "Unknown"

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
    print("\nPOI category distribution:")
    print(joined["poi_category"].value_counts().to_string())

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
